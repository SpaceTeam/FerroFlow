//! Code for managing and running sequences.

mod sequence_builder;
mod sequence_definition;
mod sequence_runner;
mod sequence_validation;

use crate::{
    events::{self, EventKind},
    nodes,
};
pub use sequence_definition::Sequence;
use sequence_runner::{SequenceCmd, SequenceRunner};

pub fn spawn_sequence_runner_thread<'scope>(
    node_manager: &'scope nodes::NodeManager<'scope>,
    event_dispatcher: &'scope events::EventDispatcher,
    scope: &'scope std::thread::Scope<'scope, '_>,
) {
    scope.spawn(move || {
        let (tx, rx) = std::sync::mpsc::channel::<events::Event>();

        let events = vec![EventKind::Sequence];
        event_dispatcher.subscribe(tx, events, "Sequence Runner thread");

        let mut sequence_runner = SequenceRunner::new(node_manager, scope);

        while let Ok(event) = rx.recv() {
            match event {
                events::Event::Shutdown => {
                    sequence_runner.control_sequence(SequenceCmd::Shutdown);
                    break;
                }
                events::Event::StartSequence { seq, abort_seq } => {
                    let result = sequence_runner.run_sequence(seq, abort_seq);
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
