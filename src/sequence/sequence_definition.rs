use anyhow::{Context, Result};
use std::{collections::HashMap, path::Path, time::Duration};

use config::{Config, File};
use serde::{Deserialize, Deserializer, de};

use crate::nodes;

pub type TimestampSec = f64;

#[derive(Debug, Deserialize, Clone)]
pub struct Sequence {
    pub name: String,
    pub globals: Globals,
    #[serde(default)]
    pub steps: Vec<Step>,
}

impl Sequence {
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let config = Config::builder().add_source(File::from(path)).build()?;

        let sequence: Self = config
            .try_deserialize()
            .with_context(|| format!("Failed to deserialize config from {}", path.display()))?;

        sequence
            .validate()
            .with_context(|| format!("Failed to validate sequence from {}", path.display()))?;
        Ok(sequence)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Globals {
    pub start_time: TimestampSec,
    pub end_time: TimestampSec,
    #[serde(deserialize_with = "duration_from_f64")]
    pub interpolation_interval: Duration,
    #[serde(default)]
    pub interpolations: HashMap<String, InterpolationMode>,
}

fn duration_from_f64<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = f64::deserialize(deserializer)?;
    if !secs.is_finite() {
        return Err(serde::de::Error::custom(
            "duration cannot be infinite or NaN",
        ));
    }
    if secs.is_sign_negative() {
        return Err(serde::de::Error::custom("duration cannot be negative"));
    }
    Ok(Duration::from_secs_f64(secs))
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum InterpolationMode {
    #[default]
    None,
    Linear,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub name: String,
    pub description: Option<String>,
    pub timestamp: TimestampSec,
    pub actions: Vec<TimedAction>,
}

impl<'de> Deserialize<'de> for Step {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct RawParamState {
            #[serde(rename = "timestamp")]
            relative_timestamp: TimestampSec,
            param: String,
            value: f64,
        }
        #[derive(Deserialize)]
        struct RawStep {
            name: String,
            description: Option<String>,
            timestamp: TimestampSec,
            set_params: Option<Vec<RawParamState>>,
            hold: Option<HoldMode>,
        }

        let raw_step = RawStep::deserialize(d)?;
        let actions = match (raw_step.hold, raw_step.set_params) {
            (Some(hold), None) => vec![TimedAction {
                timestamp: raw_step.timestamp,
                action: Action::Hold(hold),
            }],
            (None, Some(set_param)) => set_param
                .into_iter()
                .map(|param_state| TimedAction {
                    // offset relative timestamps to global timestamps
                    timestamp: raw_step.timestamp + param_state.relative_timestamp,
                    action: Action::SetParam(ParamState {
                        param: param_state.param,
                        value: param_state.value,
                    }),
                })
                .collect(),
            _ => {
                return Err(de::Error::custom(
                    "step must have exactly `hold` or `set_params`",
                ));
            }
        };

        Ok(Step {
            name: raw_step.name,
            description: raw_step.description,
            timestamp: raw_step.timestamp,
            actions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TimedAction {
    pub timestamp: TimestampSec,
    pub action: Action,
}

#[derive(Debug, Clone)]
pub enum Action {
    Hold(HoldMode),
    SetParam(ParamState),
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Deserialize, Clone)]
pub struct ParamState {
    pub param: String,
    pub value: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HoldCondition {
    pub field: String,
    #[serde(rename = "is")]
    pub comparison: FieldComparison,
    pub value: f64,
}

impl HoldCondition {
    pub fn evaluate(&self, node_manager: &nodes::NodeManager) -> bool {
        let Some(actual) = node_manager.get_value(&self.field) else {
            eprintln!("Value for field '{}' missing", self.field);
            return false;
        };
        let eps = 1e-6;
        match self.comparison {
            FieldComparison::Equal => f64_approx_eq(actual.value, self.value, eps),
            FieldComparison::NotEq => !f64_approx_eq(actual.value, self.value, eps),
            FieldComparison::Less => {
                actual.value < self.value && !f64_approx_eq(actual.value, self.value, eps)
            }
            FieldComparison::LessEq => {
                actual.value < self.value || f64_approx_eq(actual.value, self.value, eps)
            }
            FieldComparison::Greater => {
                actual.value > self.value && !f64_approx_eq(actual.value, self.value, eps)
            }
            FieldComparison::GreaterEq => {
                actual.value > self.value || f64_approx_eq(actual.value, self.value, eps)
            }
        }
    }
}

fn f64_approx_eq(a: f64, b: f64, epsilon: f64) -> bool {
    (if a > b { a - b } else { b - a }) <= epsilon
}

#[derive(Debug, Deserialize, Clone)]
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
        let err_msg = format!("{:#}", seq.unwrap_err());
        assert!(err_msg.contains("Invalid start time is after timestamp"));
    }

    #[test]
    fn test_load_invalid_interpolation_interval() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_interpolation_interval.toml"));
        assert!(seq.is_err());
        let err_msg = format!("{:#}", seq.unwrap_err());
        assert!(err_msg.contains("duration cannot be negative"));
    }

    #[test]
    fn test_load_invalid_step_times() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_step_times.toml"));
        assert!(seq.is_err());
        let err_msg = format!("{:#}", seq.unwrap_err());
        assert!(err_msg.contains("Invalid step timestamp"));
    }

    #[test]
    fn test_load_invalid_action_times() {
        let seq = Sequence::load_from_path(&sequence_path("invalid_action_times.toml"));
        assert!(seq.is_err());
        let err_msg = format!("{:#}", seq.unwrap_err());
        assert!(err_msg.contains("Invalid action timestamp"));
    }
}
