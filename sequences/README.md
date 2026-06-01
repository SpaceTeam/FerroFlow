# FerroFlow Sequences

Sequences define a timeline of automated parameter changes and (conditional) holds. They are defined as configuration files, preferably using TOML for readability.

## Components

### Globals
The `[globals]` section sets the sequence boundaries and interpolation settings:
- `start_time`: The global timestamp where the sequence begins (seconds). 
- `end_time`: The global timestamp where the sequence ends (seconds).
- `interpolation_interval`: The frequency (seconds) at which interpolated values are recalculated and sent.
- `interpolations`: A map of parameter names to their interpolation mode (`linear` or `none`). If a parameter is not specified, the default is `none`.

### Steps
Sequences are composed of `[[steps]]`. Each step occurs at a global `timestamp` (seconds) and must contain either `set_params` or a `hold` instruction.

#### Step: _Set Parameter_
Changes system parameters at specific times. The `timestamp` within each entry in `set_params` is relative to the parent step's timestamp.
- `set_params`: A list of objects with `timestamp` (relative offset in seconds), `param` name, and `value`.

#### Step: _Hold_
Pauses sequence execution at the step's timestamp.
- `hold = "always"`: The sequence pauses until manually resumed.
- `hold = [...]`: The sequence pauses if all conditions in the list are satisfied. The sequence stays paused until manually resumed.
  - `field`: The telemetry field to monitor.
  - `is`: Comparison operator (`equal`, `not_eq`, `less`, `less_eq`, `greater`, `greater_eq`).
  - `value`: The numeric value to compare against.

## Validation Rules

To ensure sequence integrity, the following rules are enforced during loading:

### Global Constraints
- `start_time` must be less than or equal to `0.0`.
- `start_time` must be less than or equal to `end_time`.
- The sequence must contain at least one step.

### Step Constraints
- **Ordering**: Steps must be defined in strictly increasing order of their `timestamp`.
- **Boundaries**: Every step's `timestamp` must fall between the global `start_time` and `end_time`.
- **Content**: Each step must contain at least one action (`hold` or `set_params`).

### Action Constraints
- **Timing**: Every action must satisfy:
  - `step.timestamp <= action.timestamp < next_step.timestamp`
  - `action.timestamp <= globals.end_time`
- **Conditional Holds**: Must contain at least one condition.

## Schema
The structure is defined in `schemas/sequence.schema.json`. Most editors/plugins will provide autocompletion and validation based on this schema. The schema should be automatically applied to any `.toml` file in a `sequence` or `sequences` directory or subdirectory.

## Sample Sequence

### TOML
```toml
name = "Ox leak check"

[globals]
start_time = -5.0
end_time = 10.0
interpolation_interval = 0.01
interpolations = { "ox_main_valve" = "linear" }

[[steps]]
name = "Initialize"
timestamp = -5.0
set_params = [
  { timestamp = 0.0, param = "ox_vent", value = 1.0 },
  { timestamp = 0.0, param = "ox_main_valve", value = 0.0 }
]

[[steps]]
name = "Pressure Check"
description = "Conditional hold"
timestamp = 1.0
hold = [
  { field = "ox_pressure", is = "greater_eq", value = 50.0 }
]

[[steps]]
name = "Manual Check"
timestamp = 3.0
hold = "always"
```