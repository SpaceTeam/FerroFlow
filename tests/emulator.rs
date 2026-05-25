mod common;

use crate::common::ShutdownGuard;
use chrono::{DateTime, Utc};
use ferro_flow::config::{Config, HeartbeatConfig};
use ferro_flow::{events, nodes, run_with_dependencies};
use liquidcan::{CanMessage, payloads::CanDataType};
use std::{io::Write, sync::mpsc, time::Duration, time::Instant};
use testcontainers::core::logs::LogFrame;
use testcontainers::{GenericImage, ImageExt, runners::SyncRunner};

const TEST_NODE_ID: u8 = 5;

#[test]
fn test_node_registration() {
    let vcan_iface = common::unique_vcan_iface();
    let _vcan = common::ensure_vcan(&vcan_iface);

    let emulator_config = ecuemulator_test_config_toml(&vcan_iface);

    let event_dispatcher = events::EventDispatcher::new();
    let node_manager = nodes::NodeManager::new(&event_dispatcher);
    let config = build_test_config(&vcan_iface);

    std::thread::scope(|s| {
        let _shutdown = ShutdownGuard {
            event_dispatcher: &event_dispatcher,
        };
        s.spawn(|| {
            run_with_dependencies(&event_dispatcher, &node_manager, config)
                .expect("application should start with test config");
        });
        let _ecuemulator_container = start_ecuemulator_container_with_config(&emulator_config);

        let start_time = Instant::now();

        loop {
            if node_manager.get_nodes().len() == 1 {
                break;
            }
            if start_time.elapsed().as_secs() > 10 {
                panic!("ECUEmulator did not register within timeout");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        let node = node_manager
            .get_nodes()
            .iter()
            .next()
            .expect("node should exist");
        assert_eq!(*node.key(), 5, "node ID should match config");
        assert_eq!(
            node.telemetry_fields.values().len(),
            1,
            "should have 1 telemetry field"
        );
        assert_eq!(
            node.parameter_fields.values().len(),
            1,
            "should have 1 parameter field"
        );
        assert_eq!(
            node.registration_info.device_name, "Emulator1",
            "device name should match config"
        );
        assert_eq!(
            node.telemetry_groups.len(),
            1,
            "Should have 1 telemetry group"
        );
        assert_eq!(
            node.telemetry_groups[&1].fields.len(),
            1,
            "Telemetry group should have 1 field"
        );
        let telemetry_field = node.telemetry_fields.iter().next().unwrap();
        assert_eq!(
            telemetry_field.1.name, "tel1",
            "Telemetry field name should match config"
        );
        assert_eq!(
            telemetry_field.1.data_type,
            CanDataType::UInt32,
            "Telemetry field datatype should match config"
        );
        assert_eq!(
            node.telemetry_groups[&1].fields[0],
            *node.telemetry_fields.keys().next().expect(""),
            "The Telemetry field should be in the group"
        );
        assert_eq!(
            node.telemetry_fields.values().len(),
            1,
            "should have 1 telemetry field"
        );
        assert_eq!(
            node.parameter_fields.values().len(),
            1,
            "should have 1 parameter field"
        );
    });
}

#[test]
fn test_telemetry_group_updates() {
    let vcan_iface = common::unique_vcan_iface();
    let _vcan = common::ensure_vcan(&vcan_iface);
    println!("Ensured {} interface exists", vcan_iface);

    let emulator_config = ecuemulator_test_config_toml(&vcan_iface);

    let event_dispatcher = events::EventDispatcher::new();
    let node_manager = nodes::NodeManager::new(&event_dispatcher);
    let config = build_test_config(&vcan_iface);
    println!("Starting application with test config: {:?}", config);

    std::thread::scope(|s| {
        let _shutdown = ShutdownGuard {
            event_dispatcher: &event_dispatcher,
        };
        s.spawn(|| {
            run_with_dependencies(&event_dispatcher, &node_manager, config)
                .expect("application should start with test config");
        });
        let _ecuemulator_container = start_ecuemulator_container_with_config(&emulator_config);

        let mut start_time = Instant::now();

        loop {
            if node_manager.get_nodes().len() == 1 {
                break;
            }
            if start_time.elapsed().as_secs() > 10 {
                panic!("ECUEmulator did not register within timeout");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        let node = node_manager
            .get_nodes()
            .iter()
            .next()
            .expect("node should exist");
        let telemetry_value_id = *node.telemetry_fields.keys().next().unwrap();

        start_time = Instant::now();
        let mut first_update_time = Instant::now();

        let mut prev_msg_time: DateTime<Utc> = DateTime::from_timestamp_nanos(0);
        let mut update_count = 0;
        loop {
            if let Some(tel_value) = node.values.get(&telemetry_value_id) {
                let msg_time = tel_value.value().0;
                if msg_time != prev_msg_time {
                    let value = tel_value.value().1.clone();
                    prev_msg_time = msg_time;
                    if update_count == 0 {
                        first_update_time = Instant::now();
                    }
                    update_count += 1;

                    assert_eq!(
                        value,
                        liquidcan::payloads::CanDataValue::UInt32(0x12345678),
                        "Telemetry value should match config"
                    );
                }
            }
            if start_time.elapsed().as_millis() > 5000
                || first_update_time.elapsed().as_millis() > 500
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        println!("Telemetry value was updated {update_count} times");
        assert!(
            (4..6).contains(&update_count),
            "Telemetry value should have been updated at least once"
        );
    });
}

#[test]
fn test_node_is_removed_when_heartbeat_responses_stop() {
    let vcan_iface = common::unique_vcan_iface();
    let _vcan = common::ensure_vcan(&vcan_iface);

    let emulator_config = ecuemulator_test_config_toml(&vcan_iface);

    let event_dispatcher = events::EventDispatcher::new();
    let (tx, heartbeat_rx) = mpsc::channel();
    event_dispatcher.subscribe(
        tx,
        vec![events::EventKind::SendCanMessage],
        "heartbeat-test-listener",
    );

    let node_manager = nodes::NodeManager::new(&event_dispatcher);
    let config = build_test_config_with_heartbeat(
        &vcan_iface,
        HeartbeatConfig {
            period: 1,
            backoff_multiplier: 2,
            max_period: 4,
            max_unanswered: 3,
        },
    );

    std::thread::scope(|s| {
        let _shutdown = ShutdownGuard {
            event_dispatcher: &event_dispatcher,
        };
        s.spawn(|| {
            run_with_dependencies(&event_dispatcher, &node_manager, config)
                .expect("application should start with test config");
        });
        let ecuemulator_container = start_ecuemulator_container_with_config(&emulator_config);

        wait_for_registered_node(&node_manager, Duration::from_secs(10));
        drain_send_can_events(&heartbeat_rx);

        ecuemulator_container
            .stop_with_timeout(Some(0))
            .expect("ecuemulator container should stop");
        drain_send_can_events(&heartbeat_rx);

        // first heartbeat at ~1s, then backoff to 2s, then 4s, then node removal after 3 unanswered heartbeats
        let (first_heartbeat_at, first_counter) =
            wait_for_heartbeat_request(&heartbeat_rx, Duration::from_secs(1100));
        let (second_heartbeat_at, second_counter) =
            wait_for_heartbeat_request(&heartbeat_rx, Duration::from_secs(2100));
        let (third_heartbeat_at, third_counter) =
            wait_for_heartbeat_request(&heartbeat_rx, Duration::from_secs(4100));

        assert_eq!(
            second_counter,
            first_counter + 1,
            "second unanswered heartbeat should increment the request counter"
        );
        assert_eq!(
            third_counter,
            first_counter + 2,
            "third unanswered heartbeat should increment the request counter"
        );
        assert!(
            second_heartbeat_at.duration_since(first_heartbeat_at) < Duration::from_millis(2100)
                && second_heartbeat_at.duration_since(first_heartbeat_at)
                    > Duration::from_millis(1900),
            "second unanswered heartbeat should be sent after ~2s"
        );

        assert!(
            third_heartbeat_at.duration_since(second_heartbeat_at) < Duration::from_millis(4100)
                && third_heartbeat_at.duration_since(second_heartbeat_at)
                    > Duration::from_millis(3900),
            "third unanswered heartbeat should be sent after backoff multiplier is applied to the heartbeat period"
        );

        // Node is removed after one regular period, as the number of unanswered heartbeats has reached the max_unanswered threshold
        wait_for_node_removal(&node_manager, TEST_NODE_ID, Duration::from_secs(1));
    });
}

fn build_test_config(can_iface: &str) -> Config {
    build_test_config_with_heartbeat(
        can_iface,
        HeartbeatConfig {
            period: 1,
            backoff_multiplier: 2,
            max_period: 10,
            max_unanswered: 3,
        },
    )
}

fn build_test_config_with_heartbeat(can_iface: &str, heartbeat: HeartbeatConfig) -> Config {
    Config {
        can_bus_interfaces: vec![can_iface.to_string()],
        heartbeat,
        database_url: "".to_string(),
    }
}

fn wait_for_registered_node(node_manager: &nodes::NodeManager<'_>, timeout: Duration) {
    let start_time = Instant::now();

    loop {
        if node_manager.get_nodes().len() == 1 {
            return;
        }
        if start_time.elapsed() > timeout {
            panic!("ECUEmulator did not register within timeout");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_node_removal(node_manager: &nodes::NodeManager<'_>, node_id: u8, expected: Duration) {
    let start_time = Instant::now();
    let max_diff = Duration::from_millis(100);

    loop {
        if node_manager.get_nodes().get(&node_id).is_none() {
            if start_time.elapsed() < expected - max_diff {
                panic!("node {node_id} was removed too early after unanswered heartbeats");
            }
            return;
        }
        if start_time.elapsed() > expected + max_diff {
            panic!(
                "node {node_id} was not removed within expected time after unanswered heartbeats"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn drain_send_can_events(rx: &mpsc::Receiver<events::Event>) {
    while rx.try_recv().is_ok() {}
}

fn wait_for_heartbeat_request(
    rx: &mpsc::Receiver<events::Event>,
    timeout: Duration,
) -> (Instant, u32) {
    let start_time = Instant::now();

    loop {
        let elapsed = start_time.elapsed();
        if elapsed >= timeout {
            panic!("heartbeat request was not dispatched within timeout");
        }

        match rx.recv_timeout(timeout - elapsed) {
            Ok(events::Event::SendCanMessage {
                receiver_node_id: TEST_NODE_ID,
                message: CanMessage::HeartbeatReq { payload },
            }) => return (Instant::now(), payload.counter),
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("heartbeat request was not dispatched within timeout");
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("heartbeat test listener disconnected");
            }
        }
    }
}

fn ecuemulator_test_config_toml(can_iface: &str) -> String {
    format!(
        r#"node_id = 5
frequency = 10
can_interface = "{can_iface}"
firmware_hash = "0x123"
liquid_hash = "0x123"
device_name = "Emulator1"

[TelemetryValues]
   [TelemetryValues.tel1]
    value = 0x12345678
    datatype = "UInt32"

[Parameters]
    [Parameters.Parameter1]
     value = 0xABAC0
     locked = false
     datatype = "UInt32"
"#
    )
}

fn start_ecuemulator_container_with_config(
    config_toml: &str,
) -> testcontainers::Container<GenericImage> {
    let container = GenericImage::new("tuwienspaceteam/ecuemulator", "latest")
        .with_network("host")
        .with_env_var("CONFIG_PATH", "/config/config.toml")
        .with_copy_to("/config/config.toml", config_toml.as_bytes().to_vec())
        .with_log_consumer(|frame: &LogFrame| {
            let mut stderr = std::io::stderr().lock();
            match frame {
                LogFrame::StdOut(bytes) | LogFrame::StdErr(bytes) => {
                    let _ = stderr.write_all(b"Container: ");
                    let _ = stderr.write_all(bytes);
                }
            }
        })
        .start()
        .expect("ecuemulator container should start");

    println!("Started ECUEmulator container (id={})", container.id());
    container
}
