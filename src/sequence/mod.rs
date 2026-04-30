//! Code for managing and running sequences.

mod sequence_builder;
mod sequence_definition;
mod sequence_runner;
mod sequence_validation;

use std::path::Path;

use crate::{
    events,
    sequence::{
        sequence_definition::Sequence,
        sequence_runner::{SequenceCmd, SequenceRunner},
    },
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
                    // TODO: replace with loading sequences from the frontend
                    let seq_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("sequences");
                    let seq = Sequence::load_from_path(&seq_dir.join(&seq_name));
                    let abort_seq = Sequence::load_from_path(&seq_dir.join(&abort_seq_name));

                    let seq = match seq {
                        Ok(seq) => seq,
                        Err(err) => {
                            eprintln!("Error while loading sequence '{seq_name}': {err:#}");
                            continue;
                        }
                    };
                    let abort_seq = match abort_seq {
                        Ok(abort_seq) => abort_seq,
                        Err(err) => {
                            eprintln!(
                                "Error while loading abort sequence '{abort_seq_name}': {err:#}"
                            );
                            continue;
                        }
                    };

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
