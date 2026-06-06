use crate::sequence::sequence_definition::{Action, HoldMode, Sequence, Step, TimedAction};

impl Sequence {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.globals.start_time <= 0.,
            "Invalid start time is after timestamp 0",
        );
        anyhow::ensure!(
            self.globals.start_time <= self.globals.end_time,
            "Invalid start time is after end time",
        );
        anyhow::ensure!(
            !self.steps.is_empty(),
            "Invalid sequence '{}' has no steps",
            self.name,
        );

        for step in &self.steps {
            self.validate_step(step)?;
        }

        // validate steps with the following step
        // steps need to:
        // - be in order
        // - have actions that are before the next step
        for window in self.steps.windows(2) {
            let step = &window[0];
            let next = &window[1];

            anyhow::ensure!(
                step.timestamp < next.timestamp,
                "Invalid step '{}', after next step",
                step.name,
            );

            for timed_action in &step.actions {
                anyhow::ensure!(
                    timed_action.timestamp < next.timestamp,
                    "Invalid action timestamp in step '{}' action '{:?}', after next step timestamp",
                    step.name,
                    timed_action.action,
                );
            }
        }

        Ok(())
    }

    // validate each step independently
    // steps need to:
    // - be sorted by timestamp
    // - be in between global start and end time
    // - have at least 1 action
    fn validate_step(&self, step: &Step) -> anyhow::Result<()> {
        anyhow::ensure!(
            step.timestamp >= self.globals.start_time,
            "Invalid step timestamp in step '{}', before global start time",
            step.name,
        );

        anyhow::ensure!(
            step.timestamp <= self.globals.end_time,
            "Invalid step timestamp in step '{}', after global end time",
            step.name,
        );

        anyhow::ensure!(
            !step.actions.is_empty(),
            "Invalid step '{}' has no actions",
            step.name,
        );

        for timed_action in &step.actions {
            self.validate_timed_action(timed_action, step)?;
        }
        Ok(())
    }

    // actions need to:
    // - be at of after the parent step
    // - before the global end time
    // conditional holds need to:
    // - have at least one condition
    fn validate_timed_action(
        &self,
        timed_action: &TimedAction,
        parent_step: &Step,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            timed_action.timestamp >= parent_step.timestamp,
            "Invalid action timestamp in step '{}' action '{:?}', before step timestamp",
            parent_step.name,
            timed_action.action,
        );
        anyhow::ensure!(
            timed_action.timestamp <= self.globals.end_time,
            "Invalid action timestamp in step '{}' action '{:?}', after global end time",
            parent_step.name,
            timed_action.action,
        );
        if let Action::Hold(HoldMode::Conditional(conditions)) = &timed_action.action {
            anyhow::ensure!(
                !conditions.is_empty(),
                "Invalid hold in step '{}' has no conditions",
                parent_step.name,
            );
        }
        Ok(())
    }
}
