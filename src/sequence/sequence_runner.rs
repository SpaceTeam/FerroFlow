use anyhow::{Result, anyhow};
use std::{
    panic,
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

use crate::{
    events::{self, EventDispatcher},
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
    Panicked,
}

pub struct SequenceRunner<'scope, 'env> {
    last_sequence_handle: Option<SequenceHandle<'scope>>,
    event_dispatcher: &'scope events::EventDispatcher,
    scope: &'scope thread::Scope<'scope, 'env>,
}

impl<'scope, 'env> SequenceRunner<'scope, 'env> {
    pub fn new(
        event_dispatcher: &'scope events::EventDispatcher,
        scope: &'scope thread::Scope<'scope, 'env>,
    ) -> Self {
        Self {
            last_sequence_handle: None,
            event_dispatcher,
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
        let event_dispatcher = self.event_dispatcher;

        let thread_handle = self.scope.spawn(move || {
            let panic_result = panic::catch_unwind( ||  {
                let seq_name = seq.name.clone();
                let abort_seq_name = abort_seq.name.clone();

                let schedule = flatten_and_interpolate(seq);
                let abort_schedule = flatten_and_interpolate(abort_seq);
                let result = Self::execute_actions(schedule, &controller_rx, event_dispatcher);

                if let Err(SequenceRunError::Aborted) = &result {
                    // TODO: add logging to the frontend
                    eprintln!("Execution of sequence '{seq_name}' was aborted, now running abort sequence '{abort_seq_name}'");
                    let _ = Self::execute_actions(abort_schedule, &controller_rx, event_dispatcher);
                }
                result
            });

            match panic_result {
                Ok(sequence_result) => sequence_result,
                Err(err) => {
                    // TODO: add logging to the frontend
                    eprintln!("Sequence Runner thread panicked with error '{:?}'", err);
                    Err(SequenceRunError::Panicked)
                }
            }
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
        #[allow(unused)] event_dispatcher: &EventDispatcher,
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
                        // TODO: implement conditions evaluation
                        HoldMode::Conditional(conditions) => {
                            conditions.iter().all(|cond| cond.evaluate())
                        }
                    };

                    if should_hold {
                        let pause_duration = Self::wait_for_resume(controller)?;
                        pause_offset += pause_duration;
                    }
                }

                Action::SetParam(_param_value) => {
                    // TODO: send can message with correct data
                    // event_dispatcher.dispatch(events::Event::SendCanMessage {
                    //     receiver_node_id: todo!(),
                    //     #[allow(unreachable_code)]
                    //     message: liquidcan::CanMessage::ParameterSetReq {
                    //         payload: liquidcan::payloads::ParameterSetReqPayload {
                    //             parameter_id: todo!(),
                    //             value: todo!(),
                    //         },
                    //     },
                    // });
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

#[cfg(test)]
mod tests {
    use super::*;
    use ntest::timeout;
    use std::{path::Path, thread, time::Duration};

    fn load_seq(name: &str) -> Sequence {
        let seq_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sequences");
        Sequence::load_from_path(&seq_dir.join(name)).expect("failed to load test sequence")
    }

    #[test]
    #[timeout(2000)]
    fn test_run_sequence_execution_completes() {
        let dispatcher = events::EventDispatcher::new();

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&dispatcher, scope);

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
            assert!(sequence_result.is_ok())
        });
    }

    #[test]
    #[timeout(2000)]
    fn test_run_sequence_hold_and_resume_completes() {
        let dispatcher = events::EventDispatcher::new();

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&dispatcher, scope);

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
        let dispatcher = events::EventDispatcher::new();

        thread::scope(|scope| {
            let mut runner = SequenceRunner::new(&dispatcher, scope);

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
            assert!(sequence_result.is_err());
            assert!(matches!(
                sequence_result.unwrap_err(),
                SequenceRunError::Aborted
            ));
        });
    }
}
