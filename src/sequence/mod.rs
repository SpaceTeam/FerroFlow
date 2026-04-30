//! Code for managing and running sequences.

mod sequence_definition;
mod sequence_runner;
mod sequence_validation;


use crate::{
    events,
    sequence::sequence_runner::{SequenceCmd, SequenceRunner},
};

pub fn spawn_sequence_runner_thread<'scope>(
    event_dispatcher: &'scope events::EventDispatcher,
    scope: &'scope std::thread::Scope<'scope, '_>,
) {
    scope.spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel::<events::Event>();
        event_dispatcher.subscribe(tx, "Sequence Runner thread");
        let mut sequence_runner = SequenceRunner::new(event_dispatcher, scope);

        while let Ok(event) = rx.recv() {
            match event {
                events::Event::Shutdown => {
                    sequence_runner.control_sequence(SequenceCmd::Shutdown);
                    break;
                }
                events::Event::StartSequence {
                    seq_name,
                    abort_seq_name,
                } => {
                    let result = sequence_runner.run_sequence(seq_name, abort_seq_name);
                    if let Err(err) = result {
                        eprintln!("Error while running sequence: {err:#}");
                    }
                }
                events::Event::PauseSequence => {
                    sequence_runner.control_sequence(SequenceCmd::Pause)
                }
                events::Event::ResumeSequence => {
                    sequence_runner.control_sequence(SequenceCmd::Resume)
                }
                events::Event::AbortSequence => {
                    sequence_runner.control_sequence(SequenceCmd::Abort)
                }
                _ => continue,
            };
        }
    });
}
