use anyhow::{Result, anyhow};
use std::{
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

use crate::{
    nodes,
    sequence::{
        sequence_builder::flatten_and_interpolate,
        sequence_definition::{Action, HoldMode, Sequence, TimedAction},
    },
};

pub struct SequenceHandle<'scope> {
    controller_tx: mpsc::Sender<SequenceCmd>,
    thread_handle: thread::ScopedJoinHandle<'scope, Result<(), SequenceRunError>>,
}

#[derive(Debug)]
pub enum SequenceCmd {
    Pause,
    Resume,
    Abort,
    Shutdown,
}

pub enum SequenceRunError {
    Aborted,
    Shutdown,
}

pub struct SequenceRunner<'scope, 'env> {
    last_sequence_handle: Option<SequenceHandle<'scope>>,
    node_manager: &'scope nodes::NodeManager<'scope>,
    scope: &'scope thread::Scope<'scope, 'env>,
}

impl<'scope, 'env> SequenceRunner<'scope, 'env> {
    pub fn new(
        node_manager: &'scope nodes::NodeManager,
        scope: &'scope thread::Scope<'scope, 'env>,
    ) -> Self {
        Self {
            last_sequence_handle: None,
            node_manager,
            scope,
        }
    }

    /// Run a sequence and an abort sequence if the sequence is aborted.
    ///
    /// Returns an error if another sequence is running.
    pub fn run_sequence(&mut self, seq: Sequence, abort_seq: Sequence) -> Result<()> {
        if self.is_sequence_running() {
            return Err(anyhow!("another sequence is still running"));
        }
        let (controller_tx, controller_rx) = mpsc::channel();
        let node_manager = self.node_manager;

        let thread_handle = self.scope.spawn(move || {
            let seq_name = seq.name.clone();
            let _panic_guard = SequencePanicGuard {
                seq_name: seq_name.clone(),
            };
            let abort_seq_name = abort_seq.name.clone();

            let schedule = flatten_and_interpolate(seq);
            let abort_schedule = flatten_and_interpolate(abort_seq);
            let result = Self::execute_actions(schedule, &controller_rx, node_manager);

            if let Err(SequenceRunError::Aborted) = &result {
                // TODO: add logging to the frontend
                eprintln!("Execution of sequence '{seq_name}' was aborted, now running abort sequence '{abort_seq_name}'");
                return Self::execute_actions(abort_schedule, &controller_rx, node_manager);
            }
            result
        });

        self.last_sequence_handle = Some(SequenceHandle {
            controller_tx,
            thread_handle,
        });

        Ok(())
    }

    /// Send pause, resume and abort commands to a running sequence.
    /// If no sequence is running, nothing happens.
    pub fn control_sequence(&mut self, cmd: SequenceCmd) {
        if !self.is_sequence_running() {
            return;
        }
        if let Some(handle) = &self.last_sequence_handle {
            let _ = handle.controller_tx.send(cmd);
        };
    }

    fn is_sequence_running(&self) -> bool {
        self.last_sequence_handle
            .as_ref()
            .is_some_and(|handle| !handle.thread_handle.is_finished())
    }

    /// Executes a list of `TimedActions`. Can be paused, resumed and aborted using the receiver parameter.
    ///
    /// Returns `Ok`, if the execution finished successfully or a `SequenceRunError`,
    /// if the execution was aborted or the controller to control the sequence was dropped.
    fn execute_actions(
        schedule: Vec<TimedAction>,
        controller: &Receiver<SequenceCmd>,
        node_manager: &nodes::NodeManager,
    ) -> Result<(), SequenceRunError> {
        let origin = Instant::now();
        let mut pause_offset = Duration::ZERO;

        for timed_action in schedule {
            // loop to wait for next action
            loop {
                let deadline =
                    origin + Duration::from_secs_f64(timed_action.timestamp) + pause_offset;
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline - now;

                match controller.recv_timeout(remaining) {
                    Ok(SequenceCmd::Resume) => {} // already running, ignore
                    Ok(SequenceCmd::Pause) => {
                        let pause_duration = Self::wait_for_resume(controller)?;
                        pause_offset += pause_duration;
                    }
                    Ok(SequenceCmd::Abort) => return Err(SequenceRunError::Aborted), // abort
                    Ok(SequenceCmd::Shutdown) => return Err(SequenceRunError::Shutdown), // server shutdown
                    Err(RecvTimeoutError::Disconnected) => return Err(SequenceRunError::Shutdown), // The caller dropped the handle without explicitly calling cancel(), abort
                    Err(RecvTimeoutError::Timeout) => break, // deadline reached, break loop
                };
            }

            match timed_action.action {
                Action::Hold(mode) => {
                    let should_hold = match mode {
                        HoldMode::Always => true,
                        HoldMode::Conditional(conditions) => {
                            conditions.iter().all(|cond| cond.evaluate(node_manager))
                        }
                    };

                    if should_hold {
                        let pause_duration = Self::wait_for_resume(controller)?;
                        pause_offset += pause_duration;
                    }
                }

                Action::SetParam(param_state) => {
                    let result = node_manager
                        .set_value(&param_state.param, serde_json::json!(param_state.value));
                    if let Err(err) = result {
                        eprintln!(
                            "Failed to set value '{}' for param '{}': {:#?}",
                            param_state.value, param_state.param, err
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Wait and block the thread until `Resume` or `Abort` is received, or the sender is disconnected.
    ///
    /// Returns the total duration spent waiting.
    fn wait_for_resume(controller: &Receiver<SequenceCmd>) -> Result<Duration, SequenceRunError> {
        let paused_at = Instant::now();
        loop {
            match controller.recv() {
                Ok(SequenceCmd::Pause) => continue, // already paused, ignore
                Ok(SequenceCmd::Resume) => return Ok(paused_at.elapsed()),
                Ok(SequenceCmd::Abort) => return Err(SequenceRunError::Aborted), // abort
                Ok(SequenceCmd::Shutdown) => return Err(SequenceRunError::Shutdown), // server shutdown
                Err(_) => return Err(SequenceRunError::Shutdown), // The caller dropped the handle without explicitly calling cancel(), shutdown
            }
        }
    }
}

struct SequencePanicGuard {
    seq_name: String,
}

impl Drop for SequencePanicGuard {
    fn drop(&mut self) {
        if thread::panicking() {
            eprintln!(
                "Sequence runner thread for sequence '{}' panicked!",
                self.seq_name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        events::{self, EventKind},
        nodes::mapping::Mapping,
    };

    use super::*;
    use liquidcan::{
        CanMessage, CanMessageId,
        payloads::{CanDataType, CanDataValue, FieldRegistrationPayload, NodeInfoResPayload},
    };
    use ntest::timeout;
    use std::{path::Path, sync::mpsc, thread, time::Duration};

    fn load_seq(name: &str) -> Sequence {
        let seq_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sequences");
        Sequence::load_from_path(&seq_dir.join(name)).expect("failed to load test sequence")
    }

    fn load_mapping() -> Mapping {
        let mapping_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("mapping")
            .join("sequences.toml");
        Mapping::load_mapping_from_file(
            mapping_path
                .to_str()
                .expect("test mapping path should be valid UTF-8"),
        )
        .expect("failed to load test mapping")
    }

    #[test]
    #[timeout(2000)]
    fn test_run_sequence_execution_completes() {
        let event_dispatcher = events::EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        event_dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");
        let node_manager = nodes::NodeManager::new(&event_dispatcher, load_mapping());
        register_sequence_test_node(&node_manager);

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&node_manager, scope);

            let seq = load_seq("valid_set_param.toml");
            let abort_seq = load_seq("abort.toml");

            runner
                .run_sequence(seq, abort_seq)
                .expect("sequence should start since no other sequence is running");

            // Wait for sequence to finish
            let handle = runner
                .last_sequence_handle
                .expect("sequence started so handle should exist");

            let join_result = handle.thread_handle.join();
            assert!(join_result.is_ok());
            let sequence_result = join_result.unwrap();
            assert!(sequence_result.is_ok());

            assert_eq!(receive_parameter_set(&rx), (5, 1, CanDataValue::UInt8(12)));
            assert_eq!(receive_parameter_set(&rx), (5, 2, CanDataValue::UInt8(12)));
        });
    }

    #[test]
    #[timeout(2000)]
    fn test_run_sequence_hold_and_resume_completes() {
        let event_dispatcher = events::EventDispatcher::new();
        let node_manager = nodes::NodeManager::new(&event_dispatcher, load_mapping());
        register_sequence_test_node(&node_manager);

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&node_manager, scope);

            let seq = load_seq("valid_hold.toml");
            let abort_seq = load_seq("abort.toml");

            runner
                .run_sequence(seq, abort_seq)
                .expect("sequence should start since no other sequence is running");

            // Wait for hold and resume
            thread::sleep(Duration::from_millis(1200));
            runner.control_sequence(SequenceCmd::Resume);

            // Wait for sequence to finish
            let handle = runner
                .last_sequence_handle
                .expect("sequence started so handle should exist");

            let join_result = handle.thread_handle.join();
            assert!(join_result.is_ok());
            let sequence_result = join_result.unwrap();
            assert!(sequence_result.is_ok())
        });
    }

    #[test]
    #[timeout(2000)]
    fn test_run_sequence_abort() {
        let event_dispatcher = events::EventDispatcher::new();
        let (tx, rx) = mpsc::channel();
        event_dispatcher.subscribe(tx, vec![EventKind::SendCanMessage], "test-send-listener");
        let node_manager = nodes::NodeManager::new(&event_dispatcher, load_mapping());
        register_sequence_test_node(&node_manager);

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&node_manager, scope);

            let seq = load_seq("valid_set_param.toml");
            let abort_seq = load_seq("abort.toml");

            runner
                .run_sequence(seq, abort_seq)
                .expect("sequence should start since no other sequence is running");

            // Wait for hold and resume
            thread::sleep(Duration::from_millis(500));
            runner.control_sequence(SequenceCmd::Abort);

            // Wait for sequence to finish
            let handle = runner
                .last_sequence_handle
                .expect("sequence started so handle should exist");

            let join_result = handle.thread_handle.join();
            assert!(join_result.is_ok());
            let sequence_result = join_result.unwrap();
            assert!(sequence_result.is_ok());
            assert_eq!(receive_parameter_set(&rx), (5, 3, CanDataValue::UInt8(1)));
        });
    }

    fn register_sequence_test_node(node_manager: &nodes::NodeManager<'_>) {
        let msg_id = CanMessageId::new()
            .with_sender_id(5)
            .with_receiver_id(liquidcan::NODE_ID_SERVER);

        node_manager
            .handle_node_info_announcement(
                msg_id,
                NodeInfoResPayload {
                    tel_count: 0,
                    par_count: 3,
                    firmware_hash: 0,
                    liquid_hash: 0,
                    device_name: "SequenceTestNode".try_into().unwrap(),
                },
            )
            .expect("test node info should register");

        for (field_id, field_name) in [(1, "servo1"), (2, "valve1"), (3, "abort")] {
            node_manager
                .handle_field_registration(
                    msg_id,
                    FieldRegistrationPayload {
                        field_id,
                        field_type: CanDataType::UInt8,
                        field_name: field_name.try_into().unwrap(),
                    },
                    false,
                )
                .expect("test parameter should register");
        }

        assert_eq!(node_manager.get_nodes().len(), 1);
    }

    fn receive_parameter_set(rx: &mpsc::Receiver<events::Event>) -> (u8, u8, CanDataValue) {
        let event = rx
            .recv_timeout(Duration::from_millis(200))
            .expect("send event should be dispatched");

        match event {
            events::Event::SendCanMessage {
                receiver_node_id,
                message:
                    CanMessage::ParameterSetReq {
                        payload:
                            liquidcan::payloads::ParameterSetReqPayload {
                                parameter_id,
                                value,
                            },
                    },
            } => (receiver_node_id, parameter_id, value),
            other => panic!("unexpected event: {other:?}"),
        }
    }
}
