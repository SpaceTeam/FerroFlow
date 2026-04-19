#[allow(unused)]
use anyhow::Result;
use anyhow::anyhow;
use std::{
    collections::HashMap,
    path::Path,
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};

use crate::{
    events::{self, EventDispatcher},
    sequence::sequence_definition::{
        Action, HoldMode, InterpolationMode, ParamValue, ScheduledAction, Sequence,
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
}

pub enum SequenceRunError {
    Aborted,
    HandleDropped,
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
    pub fn run_sequence(&mut self, seq_name: String, abort_seq_name: String) -> Result<()> {
        if self.is_sequence_running() {
            return Err(anyhow!("another sequence is still running"));
        }

        let seq_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("sequences");
        let seq = Sequence::load_from_path(&seq_dir.join(seq_name))?;
        let abort_seq = Sequence::load_from_path(&seq_dir.join(abort_seq_name))?;
        let (controller_tx, controller_rx) = mpsc::channel();
        let event_dispatcher = self.event_dispatcher;

        let thread_handle = self.scope.spawn(move || {
            let schedule = Self::build_schedule(seq);
            let abort_schedule = Self::build_schedule(abort_seq);
            let result = Self::execute_schedule(schedule, &controller_rx, event_dispatcher);

            match result {
                Ok(_) => Ok(()), // execution finished nominal
                Err(_) => Self::execute_schedule(abort_schedule, &controller_rx, event_dispatcher), // execution was aborted, start abort sequence
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

    /// Converts a `Sequence` into a list of `ScheduledAction` by interpolating any values that need it.
    /// Also, negative timestamps are removed by offsetting by the global start time,
    /// to make the execution easier by using `Duration` and `Instant` from `std`.
    fn build_schedule(seq: Sequence) -> Vec<ScheduledAction> {
        let mut scheduled_actions = Vec::new();
        let mut last_param_values = HashMap::new();
        let interpolations = &seq.globals.interpolations;

        for step in seq.steps {
            for action in step.actions {
                match action {
                    Action::Hold(_) => {
                        scheduled_actions.push(ScheduledAction {
                            // Offset timestamp to remove negative timestamps
                            timestamp: step.timestamp - seq.globals.start_time,
                            action,
                        });
                    }
                    Action::SetParam(mut pv) => {
                        // Offset timestamp to remove negative timestamps
                        pv.timestamp -= seq.globals.start_time;

                        let interpolation_mode =
                            interpolations.get(&pv.param).copied().unwrap_or_default();

                        if interpolation_mode == InterpolationMode::Linear {
                            // If previous value for this parameter existed and values changed, interpolate values
                            if let Some(last) = last_param_values.get(&pv.param) {
                                let mut points = Self::interpolate_linear(
                                    last,
                                    &pv,
                                    seq.globals.interpolation_interval,
                                );
                                scheduled_actions.append(&mut points);
                            }
                            last_param_values.insert(pv.param.clone(), pv.clone());
                        }

                        scheduled_actions.push(ScheduledAction {
                            timestamp: pv.timestamp,
                            action: Action::SetParam(pv),
                        });
                    }
                }
            }
        }

        scheduled_actions.sort_by(|f1, f2| f1.timestamp.total_cmp(&f2.timestamp));
        scheduled_actions
    }

    /// Executes a list of `ScheduledAction`, created using `build_schedule`. Can be paused, resumed and aborted using the receiver parameter.
    ///
    /// Returns `Ok`, if the execution finished successfully or a `SequenceRunError`,
    /// if the execution was aborted or the controller to control the sequence was dropped.
    fn execute_schedule(
        schedule: Vec<ScheduledAction>,
        controller: &Receiver<SequenceCmd>,
        event_dispatcher: &EventDispatcher,
    ) -> Result<(), SequenceRunError> {
        let origin = Instant::now();
        let mut pause_offset = Duration::ZERO;

        for action in schedule {
            loop {
                let deadline = origin + Duration::from_secs_f64(action.timestamp) + pause_offset;
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
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(SequenceRunError::HandleDropped);
                    }
                    // The caller dropped the handle without explicitly calling cancel(), abort
                    Err(RecvTimeoutError::Timeout) => break, // deadline reached, break loop
                };
            }

            match action.action {
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

                #[allow(unused)]
                Action::SetParam(_param_value) => {
                    // TODO: send can message with correct data
                    event_dispatcher.dispatch(events::Event::SendCanMessage {
                        receiver_node_id: todo!(),
                        message: liquidcan::CanMessage::ParameterSetReq {
                            payload: liquidcan::payloads::ParameterSetReqPayload {
                                parameter_id: todo!(),
                                value: todo!(),
                            },
                        },
                    });
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
                Ok(SequenceCmd::Resume) => return Ok(paused_at.elapsed()),
                Ok(SequenceCmd::Pause) => continue, // already paused, ignore
                Ok(SequenceCmd::Abort) => return Err(SequenceRunError::Aborted), // abort
                Err(_) => return Err(SequenceRunError::HandleDropped), // The caller dropped the handle without explicitly calling cancel(), abort
            }
        }
    }

    /// Interpolates the values using the interval, if `from` and `to` are different.
    /// The first value in the interpolation is omitted and not returned,
    /// because it's handled in `build_schedule` already.
    fn interpolate_linear(
        from: &ParamValue,
        to: &ParamValue,
        interval: f64,
    ) -> Vec<ScheduledAction> {
        if from.value == to.value {
            return vec![];
        }

        let mut interpolated_values = Vec::new();

        let timespan = to.timestamp - from.timestamp;
        let tick_count = (timespan / interval).floor() as usize;

        for tick in 1..=tick_count {
            let tick_timestamp = from.timestamp + tick as f64 * interval;
            if tick_timestamp >= to.timestamp {
                break;
            }
            let alpha = (tick_timestamp - from.timestamp) / timespan;
            let value = from.value + alpha * (to.value - from.value);
            interpolated_values.push(ScheduledAction {
                timestamp: tick_timestamp,
                action: Action::SetParam(ParamValue {
                    timestamp: tick_timestamp,
                    param: from.param.clone(),
                    value,
                }),
            });
        }

        interpolated_values
    }
}
