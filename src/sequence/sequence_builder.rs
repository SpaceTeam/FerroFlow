use crate::sequence::sequence_definition::{
    Action, InterpolationMode, ParamState, Sequence, TimedAction, TimestampSec,
};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

#[derive(Debug, Clone, Copy)]
struct TimedValue {
    timestamp: TimestampSec,
    value: f64,
}

pub fn flatten_and_interpolate(seq: Sequence) -> Vec<TimedAction> {
    let mut final_actions = Vec::with_capacity(seq.steps.len());
    let mut last_param_states: HashMap<String, TimedValue> = HashMap::new();

    let flattened_actions = seq.steps.into_iter().flat_map(|step| step.actions);

    for mut timed_action in flattened_actions {
        // offset timestamps by global start time to remove negative times
        timed_action.timestamp -= seq.globals.start_time;

        let Action::SetParam(param_state) = &timed_action.action else {
            // Action is not a SetParam action
            final_actions.push(timed_action);
            continue;
        };

        let interpolation_mode = seq.globals.interpolations.get(&param_state.param);
        let Some(&InterpolationMode::Linear) = interpolation_mode else {
            // Action does not need to be interpolated
            final_actions.push(timed_action);
            continue;
        };

        let new_param_value = TimedValue {
            timestamp: timed_action.timestamp,
            value: param_state.value,
        };
        if let Some(last_param_value) = last_param_states.remove(&param_state.param) {
            let mut interpolated = interpolate_linear(
                last_param_value,
                new_param_value,
                seq.globals.interpolation_interval,
            );
            // Remove the first and last element since they are already contained in the sequence definition
            interpolated.pop_front();
            interpolated.pop_back();
            let interpolated_actions = interpolated.into_iter().map(|timed_value| TimedAction {
                timestamp: timed_value.timestamp,
                action: Action::SetParam(ParamState {
                    param: param_state.param.clone(),
                    value: timed_value.value,
                }),
            });

            final_actions.extend(interpolated_actions);
        }
        last_param_states.insert(param_state.param.clone(), new_param_value);
        final_actions.push(timed_action);
    }

    final_actions
}

/// Interpolates the values using the interval, if the values of `from` and `to` are different.
///
/// Returns a list of `TimedValues`, including the `from` and `to` values (inclusive).
/// Returns an empty list, if `from` and `to` have the same value or the interpolation interval is to large (interval <= timespan/2).
fn interpolate_linear(
    from: TimedValue,
    to: TimedValue,
    interval: Duration,
) -> VecDeque<TimedValue> {
    if from.value == to.value {
        return VecDeque::new();
    }

    let interval = interval.as_secs_f64();
    let timespan = to.timestamp - from.timestamp;

    if interval > timespan / 2. {
        return VecDeque::new();
    }

    let mut interpolated_values = VecDeque::new();
    let tick_count = (timespan / interval).floor() as usize;

    for tick in 0..=tick_count {
        let tick_timestamp = from.timestamp + tick as f64 * interval;
        let alpha = (tick_timestamp - from.timestamp) / timespan;
        let value = from.value + alpha * (to.value - from.value);
        interpolated_values.push_back(TimedValue {
            timestamp: tick_timestamp,
            value,
        });
    }

    interpolated_values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interpolate_linear() {
        let from = TimedValue {
            timestamp: 0.,
            value: 0.,
        };
        let to = TimedValue {
            timestamp: 10.,
            value: 100.,
        };
        let interval = Duration::from_secs(2);
        let results = interpolate_linear(from, to, interval);

        let timestamps = [0., 2., 4., 6., 8., 10.];
        let values = [0., 20., 40., 60., 80., 100.];

        assert_eq!(results.len(), 6);

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result.timestamp, timestamps[i]);
            assert_eq!(result.value, values[i]);
        }
    }

    #[test]
    fn test_interpolate_linear_same_value() {
        let from = TimedValue {
            timestamp: 0.,
            value: 100.,
        };
        let to = TimedValue {
            timestamp: 10.,
            value: 100.,
        };
        let interval = Duration::from_secs(2);
        let results = interpolate_linear(from, to, interval);
        assert!(
            results.is_empty(),
            "Should return empty vec because values are identical"
        );
    }

    #[test]
    fn test_interpolate_interval_too_large() {
        let from = TimedValue {
            timestamp: 0.,
            value: 0.,
        };
        let to = TimedValue {
            timestamp: 10.,
            value: 100.,
        };
        let interval = Duration::from_secs(7);
        let results = interpolate_linear(from, to, interval);
        assert!(
            results.is_empty(),
            "Should return empty vec because interpolation interval is too large"
        );
    }
}
