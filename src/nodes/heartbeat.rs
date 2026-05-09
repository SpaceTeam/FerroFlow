use std::{
    sync::mpsc::{self, RecvTimeoutError},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use liquidcan::{CanMessage, CanMessageId, payloads::HeartbeatPayload};

use crate::{
    config::HeartbeatConfig,
    events::{self, EventKind},
};

use super::node_manager::NodeManager;

pub fn spawn_heartbeat_thread<'a>(
    node_manager: &'a NodeManager<'a>,
    heartbeat_config: &'a HeartbeatConfig,
    event_dispatcher: &'a events::EventDispatcher,
    scope: &'a std::thread::Scope<'a, '_>,
) {
    let (tx, rx) = mpsc::channel::<events::Event>();
    let events = vec![EventKind::Shutdown];

    event_dispatcher.subscribe(tx, events, "Heartbeat thread");

    scope.spawn(move || {
        if let Err(error) =
            dispatch_heartbeat_requests(node_manager, event_dispatcher, heartbeat_config)
        {
            eprintln!("Error dispatching heartbeat requests: {error:#}");
        }

        let period_duration = Duration::from_secs(heartbeat_config.period as u64);

        let mut next_heartbeat_at = Instant::now() + period_duration;

        loop {
            match rx.recv_timeout(next_heartbeat_at - Instant::now()) {
                Ok(events::Event::Shutdown) => break,
                Err(RecvTimeoutError::Timeout) => {
                    if let Err(error) = dispatch_heartbeat_requests(
                        node_manager,
                        event_dispatcher,
                        heartbeat_config,
                    ) {
                        eprintln!("Error dispatching heartbeat requests: {error:#}");
                    }

                    next_heartbeat_at += period_duration;

                    // edge case: if next_heartbeat_at is already in the past, skip to now.
                    if next_heartbeat_at < Instant::now() {
                        next_heartbeat_at = Instant::now();
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
                Ok(_) => {}
            }
        }
    });
}

pub fn handle_heartbeat_res(
    node_manager: &NodeManager,
    can_msg_id: CanMessageId,
    payload: HeartbeatPayload,
) -> Result<()> {
    let timestamp = Utc::now();
    let node_id = can_msg_id.sender_id();

    let node = node_manager.get_nodes().get(&node_id).with_context(|| {
        format!(
            "received heartbeat response for node {} but it is not registered",
            node_id
        )
    })?;

    let mut latest_heartbeat = node
        .latest_heartbeat_received
        .write()
        .map_err(|error| anyhow!("RwLock was poisoned: {}", error))?;

    *latest_heartbeat = Some((timestamp, payload.counter));

    Ok(())
}

fn dispatch_heartbeat_requests(
    node_manager: &NodeManager,
    event_dispatcher: &events::EventDispatcher,
    heartbeat_config: &HeartbeatConfig,
) -> Result<()> {
    let now = Utc::now();
    let mut expired_nodes = Vec::new();

    for node_entry in node_manager.get_nodes().iter() {
        let node_id = *node_entry.key();
        let latest_heartbeat_received = node_entry
            .latest_heartbeat_received
            .read()
            .map_err(|error| anyhow!("RwLock was poisoned: {}", error))?
            .to_owned();

        let mut latest_heartbeat_sent = node_entry
            .latest_heartbeat_sent
            .write()
            .map_err(|error| anyhow!("RwLock was poisoned: {}", error))?;
        let latest_sent = latest_heartbeat_sent.to_owned();

        let unanswered_heartbeats =
            unanswered_heartbeat_count(latest_sent, latest_heartbeat_received);

        if unanswered_heartbeats >= heartbeat_config.max_unanswered {
            expired_nodes.push((node_id, unanswered_heartbeats));
            continue;
        }

        if !heartbeat_is_due(now, latest_sent, unanswered_heartbeats, heartbeat_config) {
            continue;
        }

        let next_heartbeat = latest_heartbeat_sent
            .map(|(_, counter)| counter + 1)
            .unwrap_or(0);

        *latest_heartbeat_sent = Some((now, next_heartbeat));

        event_dispatcher.dispatch(events::Event::SendCanMessage {
            receiver_node_id: node_id,
            message: CanMessage::HeartbeatReq {
                payload: HeartbeatPayload {
                    counter: next_heartbeat,
                },
            },
        });
    }

    for (node_id, unanswered_heartbeats) in expired_nodes {
        if node_manager.get_nodes().remove(&node_id).is_some() {
            eprintln!(
                "Removing CAN node {node_id}: {unanswered_heartbeats} unanswered heartbeat requests"
            );
        }
    }

    Ok(())
}

fn heartbeat_is_due(
    now: DateTime<Utc>,
    latest_sent: Option<(DateTime<Utc>, u32)>,
    unanswered_heartbeats: u32,
    heartbeat_config: &HeartbeatConfig,
) -> bool {
    let Some((latest_sent_at, _)) = latest_sent else {
        return true;
    };

    let elapsed = now
        .signed_duration_since(latest_sent_at)
        .to_std()
        .unwrap_or_default();

    elapsed >= interval_after_unanswered(heartbeat_config, unanswered_heartbeats)
}

fn unanswered_heartbeat_count(
    latest_sent: Option<(DateTime<Utc>, u32)>,
    latest_received: Option<(DateTime<Utc>, u32)>,
) -> u32 {
    let Some((_, sent_counter)) = latest_sent else {
        return 0;
    };

    match latest_received {
        // Nodes respond to heartbeat requests with the counter incremented by 1.
        Some((_, received_counter)) => sent_counter - (received_counter - 1),
        None => sent_counter + 1,
    }
}

pub fn interval_after_unanswered(
    heartbeat_config: &HeartbeatConfig,
    unanswered_heartbeats: u32,
) -> Duration {
    let multiplier = heartbeat_config
        .backoff_multiplier
        .saturating_pow(unanswered_heartbeats);
    let interval = heartbeat_config.period.saturating_mul(multiplier);

    Duration::from_secs(interval.min(heartbeat_config.max_period) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
    use liquidcan::payloads::NodeInfoResPayload;

    fn test_heartbeat_config() -> HeartbeatConfig {
        HeartbeatConfig {
            period: 1,
            backoff_multiplier: 2,
            max_period: 60,
            max_unanswered: 3,
        }
    }

    fn register_test_node(manager: &NodeManager, node_id: u8) {
        let message_id = CanMessageId::new().with_sender_id(node_id);
        manager
            .handle_node_info_announcement(
                message_id,
                NodeInfoResPayload {
                    tel_count: 0,
                    par_count: 0,
                    firmware_hash: 0,
                    liquid_hash: 0,
                    device_name: "test-node".try_into().unwrap(),
                },
            )
            .expect("test node should register");
    }

    fn drain_heartbeat_counter(rx: &mpsc::Receiver<events::Event>) -> u32 {
        let event = rx
            .recv_timeout(Duration::from_millis(50))
            .expect("heartbeat request should be dispatched");

        let events::Event::SendCanMessage { message, .. } = event else {
            panic!("expected SendCanMessage event");
        };

        let CanMessage::HeartbeatReq { payload } = message else {
            panic!("expected heartbeat request");
        };

        payload.counter
    }

    fn make_latest_heartbeat_sent_old(manager: &NodeManager, node_id: u8, seconds: i64) {
        let node = manager
            .get_nodes()
            .get(&node_id)
            .expect("test node should exist");
        let mut latest_sent = node
            .latest_heartbeat_sent
            .write()
            .expect("heartbeat sent lock should not be poisoned");
        let Some((sent_at, _)) = latest_sent.as_mut() else {
            panic!("test node should have heartbeat sent state");
        };

        *sent_at = Utc::now() - ChronoDuration::seconds(seconds);
    }

    #[test]
    fn heartbeat_config_caps_backoff_interval() {
        let config = HeartbeatConfig {
            period: 1,
            backoff_multiplier: 2,
            max_period: 5,
            max_unanswered: 3,
        };

        assert_eq!(
            interval_after_unanswered(&config, 0),
            Duration::from_secs(1)
        );
        assert_eq!(
            interval_after_unanswered(&config, 1),
            Duration::from_secs(2)
        );
        assert_eq!(
            interval_after_unanswered(&config, 2),
            Duration::from_secs(4)
        );
        assert_eq!(
            interval_after_unanswered(&config, 3),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn unanswered_heartbeats_backoff_and_eventually_evict_node() {
        let dispatcher = events::EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![events::EventKind::SendCanMessage], "test-listener");

        let manager = NodeManager::new(&dispatcher);
        register_test_node(&manager, 5);

        let config = test_heartbeat_config();

        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert_eq!(drain_heartbeat_counter(&rx), 0);

        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());

        make_latest_heartbeat_sent_old(&manager, 5, 2);
        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert_eq!(drain_heartbeat_counter(&rx), 1);

        make_latest_heartbeat_sent_old(&manager, 5, 4);
        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert_eq!(drain_heartbeat_counter(&rx), 2);

        make_latest_heartbeat_sent_old(&manager, 5, 8);
        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        assert!(manager.get_nodes().get(&5).is_none());
    }

    #[test]
    fn heartbeat_response_resets_unanswered_backoff() {
        let dispatcher = events::EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        dispatcher.subscribe(tx, vec![events::EventKind::SendCanMessage], "test-listener");

        let manager = NodeManager::new(&dispatcher);
        register_test_node(&manager, 5);

        let config = test_heartbeat_config();

        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert_eq!(drain_heartbeat_counter(&rx), 0);

        let message_id = CanMessageId::new().with_sender_id(5);
        handle_heartbeat_res(&manager, message_id, HeartbeatPayload { counter: 1 }).unwrap();

        make_latest_heartbeat_sent_old(&manager, 5, 1);
        dispatch_heartbeat_requests(&manager, &dispatcher, &config).unwrap();
        assert_eq!(drain_heartbeat_counter(&rx), 1);
    }
}
