#![allow(unused)]

use anyhow::{Context, Result};
use std::{collections::HashMap, path::Path};

use config::{Config, File};
use serde::{Deserialize, Deserializer, de};

type TimestampSec = f64;

#[derive(Debug, Deserialize)]
pub struct Sequence {
    pub name: String,
    pub globals: Globals,
    #[serde(default)]
    pub steps: Vec<Step>,
}

impl Sequence {
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let config = Config::builder().add_source(File::from(path)).build()?;

        let mut sequence: Self = config
            .try_deserialize()
            .with_context(|| format!("Failed to deserialize config from {}", path.display()))?;

        sequence
            .steps
            .sort_by(|a, b| a.timestamp.total_cmp(&b.timestamp));
        sequence.validate()?;
        Ok(sequence)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.globals.start_time <= 0.0,
            "Invalid start time is after timestamp 0",
        );
        anyhow::ensure!(
            self.globals.start_time <= self.globals.end_time,
            "Invalid start time is after end time",
        );
        anyhow::ensure!(
            self.globals.interpolation_interval > 0.0,
            "Invalid interpolations interval must be greater 0",
        );

        for window in self.steps.windows(2) {
            let step = &window[0];
            let next = &window[1];

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

            for action in &step.actions {
                match action {
                    Action::SetParam(fv) => {
                        anyhow::ensure!(
                            fv.timestamp >= step.timestamp,
                            "Invalid action timestamp in step '{}' action '{}', before step timestamp",
                            step.name,
                            fv.param,
                        );

                        anyhow::ensure!(
                            fv.timestamp <= self.globals.end_time,
                            "Invalid action timestamp in step '{}' action '{}', after global end time",
                            step.name,
                            fv.param,
                        );

                        anyhow::ensure!(
                            fv.timestamp <= next.timestamp,
                            "Invalid action timestamp in step '{}' action '{}', after next step timestamp",
                            step.name,
                            fv.param,
                        );
                    }
                    Action::Hold(hold_mode) => {
                        if let HoldMode::Conditional(conditions) = hold_mode {
                            anyhow::ensure!(
                                !conditions.is_empty(),
                                "Invalid hold in step '{}' has conditions",
                                step.name,
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct Globals {
    pub start_time: TimestampSec,
    pub end_time: TimestampSec,
    pub interpolation_interval: TimestampSec,
    #[serde(default)]
    pub interpolations: HashMap<String, InterpolationMode>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum InterpolationMode {
    #[default]
    None,
    Linear,
}

#[derive(Debug)]
pub struct Step {
    pub name: String,
    pub description: Option<String>,
    pub timestamp: TimestampSec,
    pub actions: Vec<Action>,
}

impl<'de> Deserialize<'de> for Step {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RawStep {
            name: String,
            #[serde(default)]
            description: Option<String>,
            timestamp: TimestampSec,
            #[serde(default)]
            set_params: Vec<ParamValue>,
            hold: Option<HoldMode>,
        }

        let raw = RawStep::deserialize(d)?;
        let actions = match (raw.hold, raw.set_params.is_empty()) {
            (Some(mode), true) => vec![Action::Hold(mode)],
            (Some(_), false) => {
                return Err(de::Error::custom(
                    "step can either have `hold` or `set_Params`",
                ));
            }
            (None, _) => raw
                .set_params
                .into_iter()
                .map(|fv| {
                    Action::SetParam(ParamValue {
                        timestamp: raw.timestamp + fv.timestamp,
                        param: fv.param,
                        value: fv.value,
                    })
                })
                .collect(),
        };

        Ok(Step {
            name: raw.name,
            description: raw.description,
            timestamp: raw.timestamp,
            actions,
        })
    }
}

#[derive(Debug, Deserialize)]
pub enum Action {
    Hold(HoldMode),
    SetParam(ParamValue),
}

#[derive(Debug)]
pub enum HoldMode {
    Always,
    Conditional(Vec<HoldCondition>),
}

impl<'de> Deserialize<'de> for HoldMode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawHoldMode {
            Always(String),
            Conditional(Vec<HoldCondition>),
        }

        let raw = RawHoldMode::deserialize(d)?;
        match raw {
            RawHoldMode::Always(s) if s.eq_ignore_ascii_case("always") => Ok(HoldMode::Always),
            RawHoldMode::Always(s) => Err(de::Error::unknown_variant(&s, &["always"])),
            RawHoldMode::Conditional(c) => Ok(HoldMode::Conditional(c)),
        }
    }
}

#[derive(Debug)]
pub struct ScheduledAction {
    pub timestamp: TimestampSec,
    pub action: Action,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ParamValue {
    pub timestamp: TimestampSec,
    pub param: String,
    pub value: f64,
}

#[derive(Debug, Deserialize)]
pub struct HoldCondition {
    field: String,
    is: FieldComparison,
    value: f64,
}

impl HoldCondition {
    pub fn evaluate(&self) -> bool {
        // TODO: evaluate condition if it's true based on the actual field values
        todo!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldComparison {
    Equal,
    NotEq,
    Less,
    LessEq,
    Greater,
    GreaterEq,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sequence_path(filename: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("sequences")
            .join(filename)
    }

    #[test]
    fn test_load_success() {
        let seq = Sequence::load_from_path(&sequence_path("valid.toml"))
            .expect("valid sequence should be loaded");
        assert_eq!("Test Sequence Valid", seq.name);
    }

    #[test]
    fn test_load_invalid_global_times() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_global_times.toml"));
        assert!(seq.is_err());
    }

    #[test]
    fn test_load_invalid_interpolation_interval() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_interpolation_interval.toml"));
        assert!(seq.is_err());
    }

    #[test]
    fn test_load_invalid_step_times() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_step_times.toml"));
        assert!(seq.is_err());
    }

    #[test]
    fn test_load_invalid_action_times() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_action_times.toml"));
        assert!(seq.is_err());
    }
}
