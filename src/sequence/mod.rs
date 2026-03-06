//! Code for managing and running sequences.

use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

pub struct Sequence {
    name: String,
    steps: Vec<SequenceStep>,
}

struct SequenceStep {
    description: String,
    delay_from_start_ms: u64,
    action: (), // TODO: how are steps defined?
}

pub struct SequenceHandle {
    cancel_tx: mpsc::Sender<()>,
    thread_handle: thread::JoinHandle<()>,
}

impl SequenceHandle {
    /// Signals the sequence to stop executing further steps.
    pub fn cancel(self) {
        let _ = self.cancel_tx.send(());
    }

    /// Blocks until the sequence finishes (or is cancelled).
    pub fn wait(self) -> thread::Result<()> {
        self.thread_handle.join()
    }
}

pub fn run_sequence(mut seq: Sequence) -> SequenceHandle {
    // Create a channel for our interrupt signal
    let (cancel_tx, cancel_rx) = mpsc::channel();

    // Sort steps.
    // TODO: We could probably require that they are already sorted at this point.
    seq.steps.sort_by_key(|s| s.delay_from_start_ms);

    let thread_handle = thread::spawn(move || {
        println!("Starting sequence: {}", seq.name);

        let start_time = Instant::now();

        for step in seq.steps {
            // Calculate the absolute target time for this specific step
            let target_time = start_time + Duration::from_millis(step.delay_from_start_ms);
            let now = Instant::now();

            // If the target time is in the future, we need to wait
            if target_time > now {
                let wait_duration = target_time - now;

                // recv_timeout blocks until a message is received OR the timeout is reached.
                match cancel_rx.recv_timeout(wait_duration) {
                    Ok(_) => {
                        println!("Sequence '{}' interrupted! Aborting.", seq.name);
                        return;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        // The caller dropped the handle without explicitly calling cancel().
                        println!("Sequence handle dropped. Aborting '{}'.", seq.name);
                        return;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Timeout reached without interruption. Run the step.
                    }
                }
            }

            println!("Executing step: {}", step.description);
        }

        println!("Sequence '{}' completed successfully.", seq.name);
    });

    SequenceHandle {
        cancel_tx,
        thread_handle,
    }
}
