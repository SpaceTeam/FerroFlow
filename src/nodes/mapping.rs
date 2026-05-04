use anyhow::{Context, bail, ensure};
use liquidcan::payloads::{CanDataType, CanDataValue};
use serde::Deserialize;
use std::collections::HashSet;
use toml::Value;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct NodeMapping {
    pub id: String,
    pub description: String,
    pub mapping: Vec<MappingEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MappingEntry {
    pub name: String,
    pub raw: RawField,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(default)]
    pub value: ValueParams,
    #[serde(default)]
    pub logical: Vec<LogicalRule>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawField {
    pub node: String,
    pub field: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ValueParams {
    pub slope: f64,
    pub offset: f64,
    #[serde(default)]
    pub unit: String,
}

impl Default for ValueParams {
    fn default() -> Self {
        Self {
            slope: 1.0,
            offset: 0.0,
            unit: "".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogicalRule {
    pub range: LogicalRangeConfig,
    pub value: Value,
    #[serde(default)]
    pub color: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogicalRangeConfig {
    /// Inclusive lower bound by default. If omitted, the range is unbounded below.
    #[serde(default)]
    pub min: Option<f64>,
    /// Exclusive upper bound by default. If omitted, the range is unbounded above.
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default = "default_min_inclusive")]
    pub min_inclusive: bool,
    #[serde(default)]
    pub max_inclusive: bool,
}

fn default_min_inclusive() -> bool {
    true
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Telemetry,
    Parameter,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalValue {
    pub value: Value,
    pub color: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MappedValue {
    pub value: f64,
    pub unit: String,
}

impl NodeMapping {
    pub fn load_mapping_from_file(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::default());
        }

        let toml_str = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read mapping config file at {}", path))?;
        Self::parse_mapping(&toml_str)
    }

    pub fn parse_mapping(toml_str: &str) -> anyhow::Result<Self> {
        let config = toml::from_str::<Self>(toml_str)
            .map_err(|err| anyhow::anyhow!("Failed to parse mapping config: {}", err))?;

        config.validate().with_context(|| {
            format!(
                "Mapping validation failed for config with id: {}",
                config.id
            )
        })?;

        Ok(config)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut names = HashSet::new();
        let mut raw_fields = HashSet::new();
        for mapping in &self.mapping {
            // check name is not empty and unique
            ensure!(
                !mapping.name.trim().is_empty(),
                "mapping name must not be empty"
            );
            if !names.insert(mapping.name.as_str()) {
                anyhow::bail!("Duplicate mapping name: {}", mapping.name);
            }

            // check raw.node and raw.field are not empty and unique in combination
            let raw_id = (mapping.raw.node.as_str(), mapping.raw.field.as_str());
            ensure!(
                !raw_id.0.trim().is_empty(),
                "mapping {} has empty raw.node",
                mapping.name
            );
            ensure!(
                !raw_id.1.trim().is_empty(),
                "mapping {} has empty raw.field",
                mapping.name
            );
            if !raw_fields.insert(raw_id) {
                anyhow::bail!(
                    "Duplicate raw field mapping for node '{}' field '{}'",
                    mapping.raw.node,
                    mapping.raw.field
                );
            }
            mapping.validate()?;
        }

        Ok(())
    }

    pub fn get_mapping_for_name(&self, name: &str) -> Option<&MappingEntry> {
        self.mapping.iter().find(|m| m.name == name)
    }

    pub fn get_mapping_for_raw(
        &self,
        node: &str,
        field: &str,
        field_type: FieldType,
    ) -> Option<&MappingEntry> {
        self.mapping.iter().find(|mapping| {
            mapping.raw.node == node
                && mapping.raw.field == field
                && mapping.field_type == field_type
        })
    }
}

impl MappingEntry {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.raw.node.trim().is_empty(),
            "mapping {} must specify raw.node",
            self.name
        );
        ensure!(
            !self.raw.field.trim().is_empty(),
            "mapping {} must specify raw.field",
            self.name
        );

        ensure!(
            self.value.slope.is_finite(),
            "mapping {} has a non-finite slope",
            self.name
        );
        ensure!(
            self.value.slope != 0.0,
            "mapping {} has a slope of zero, which is not allowed",
            self.name
        );
        ensure!(
            self.value.offset.is_finite(),
            "mapping {} has a non-finite offset",
            self.name
        );

        self.validate_logical_rules()?;

        Ok(())
    }

    /// Validates that logical rules form an unambiguous partition of all mapped values.
    ///
    /// Empty logical rules are allowed. Once any logical rule is present, the ranges must be
    /// non-empty, non-overlapping, and exhaustive over `(-inf, inf)` so every mapped value has
    /// exactly one logical value.
    fn validate_logical_rules(&self) -> anyhow::Result<()> {
        if self.logical.is_empty() {
            return Ok(());
        }

        let mut covered_ranges = Vec::new();

        for (index, rule) in self.logical.iter().enumerate() {
            let range = rule.range.to_logical_range().with_context(|| {
                format!(
                    "Logical rule {} for mapping {} has an invalid range",
                    index + 1,
                    self.name
                )
            })?;

            if !range.is_non_empty() {
                bail!(
                    "Logical rule {} for mapping {} has an empty range {}",
                    index + 1,
                    self.name,
                    range.describe()
                );
            }

            for (covered_index, covered_range) in covered_ranges.iter().enumerate() {
                if let Some(overlap) = range.intersection(covered_range) {
                    bail!(
                        "Logical rule {} for mapping {} overlaps with rule {} in {}; overlapping ranges are ambiguous",
                        index + 1,
                        self.name,
                        covered_index + 1,
                        overlap.describe()
                    );
                }
            }

            covered_ranges.push(range);
        }

        let uncovered_ranges = covered_ranges.iter().fold(
            vec![LogicalRange::all()],
            |remaining_uncovered_ranges, covered_range| {
                remaining_uncovered_ranges
                    .into_iter()
                    .flat_map(|range| range.difference(covered_range))
                    .collect::<Vec<_>>()
            },
        );

        if let Some(uncovered_range) = uncovered_ranges.first() {
            bail!(
                "Logical rules for mapping {} are not exhaustive; values in {} are not matched",
                self.name,
                uncovered_range.describe()
            );
        }

        Ok(())
    }

    /// Applies the linear mapping `mapped = raw * slope + offset`.
    pub fn mapped_value(&self, raw_value: &CanDataValue) -> anyhow::Result<MappedValue> {
        let numeric_raw_value = can_data_value_to_f64(raw_value)?;

        Ok(MappedValue {
            unit: self.value.unit.clone(),
            value: numeric_raw_value * self.value.slope + self.value.offset,
        })
    }

    /// Inverts the linear mapping and converts the result to the concrete CAN data type.
    pub fn raw_value_from_mapped(
        &self,
        mapped_value: f64,
        data_type: CanDataType,
    ) -> anyhow::Result<CanDataValue> {
        ensure!(
            mapped_value.is_finite(),
            "mapped value for {} must be finite",
            self.name
        );

        ensure!(
            self.value.slope != 0.0,
            "cannot invert mapping {} because slope is zero",
            self.name
        );

        can_data_value_from_f64(
            (mapped_value - self.value.offset) / self.value.slope,
            data_type,
        )
    }

    pub fn logical_value(&self, mapped_value: f64) -> Option<LogicalValue> {
        self.logical
            .iter()
            .find(|rule| rule.matches(mapped_value))
            .map(|rule| LogicalValue {
                value: rule.value.clone(),
                color: rule.color.clone(),
            })
    }
}

impl LogicalRule {
    fn matches(&self, mapped_value: f64) -> bool {
        self.range
            .to_logical_range()
            .is_ok_and(|range| range.contains(mapped_value))
    }
}

impl LogicalRangeConfig {
    /// Converts the TOML range representation into the internal interval type
    fn to_logical_range(&self) -> anyhow::Result<LogicalRange> {
        if let Some(min) = self.min {
            ensure!(min.is_finite(), "range min must be finite");
        }
        if let Some(max) = self.max {
            ensure!(max.is_finite(), "range max must be finite");
        }

        Ok(LogicalRange::new(
            RangeBound {
                value: self.min,
                inclusive: self.min_inclusive,
            },
            RangeBound {
                value: self.max,
                inclusive: self.max_inclusive,
            },
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LogicalRange {
    lower: RangeBound,
    upper: RangeBound,
}

impl LogicalRange {
    /// The complete domain
    fn all() -> Self {
        Self::new(
            RangeBound::negative_infinity(),
            RangeBound::positive_infinity(),
        )
    }

    fn new(lower: RangeBound, upper: RangeBound) -> Self {
        Self { lower, upper }
    }

    fn contains(&self, value: f64) -> bool {
        let above_lower = match self.lower.value {
            Some(lower) if self.lower.inclusive => value >= lower,
            Some(lower) => value > lower,
            None => true,
        };
        let below_upper = match self.upper.value {
            Some(upper) if self.upper.inclusive => value <= upper,
            Some(upper) => value < upper,
            None => true,
        };

        above_lower && below_upper
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let intersection = Self::new(
            RangeBound::max_lower(self.lower, other.lower),
            RangeBound::min_upper(self.upper, other.upper),
        );

        intersection.is_non_empty().then_some(intersection)
    }

    fn difference(&self, other: &Self) -> Vec<Self> {
        let Some(intersection) = self.intersection(other) else {
            return vec![*self];
        };

        let mut remaining = Vec::new();

        if intersection.lower.value.is_some() {
            let left = Self::new(
                self.lower,
                RangeBound {
                    value: intersection.lower.value,
                    inclusive: !intersection.lower.inclusive,
                },
            );
            if left.is_non_empty() {
                remaining.push(left);
            }
        }

        if intersection.upper.value.is_some() {
            let right = Self::new(
                RangeBound {
                    value: intersection.upper.value,
                    inclusive: !intersection.upper.inclusive,
                },
                self.upper,
            );
            if right.is_non_empty() {
                remaining.push(right);
            }
        }

        remaining
    }

    fn is_non_empty(&self) -> bool {
        match (self.lower.value, self.upper.value) {
            (Some(lower), Some(upper)) if lower > upper => false,
            (Some(lower), Some(upper)) if lower == upper => {
                self.lower.inclusive && self.upper.inclusive
            }
            _ => true,
        }
    }

    fn describe(&self) -> String {
        let lower = match self.lower.value {
            Some(value) if self.lower.inclusive => format!("[{value}"),
            Some(value) => format!("({value}"),
            None => "(-inf".to_string(),
        };
        let upper = match self.upper.value {
            Some(value) if self.upper.inclusive => format!("{value}]"),
            Some(value) => format!("{value})"),
            None => "inf)".to_string(),
        };

        format!("{lower}, {upper}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RangeBound {
    value: Option<f64>,
    inclusive: bool,
}

impl RangeBound {
    fn negative_infinity() -> Self {
        Self {
            value: None,
            inclusive: false,
        }
    }

    fn positive_infinity() -> Self {
        Self {
            value: None,
            inclusive: false,
        }
    }

    fn finite(value: f64, inclusive: bool) -> Self {
        Self {
            value: Some(value),
            inclusive,
        }
    }

    fn max_lower(left: Self, right: Self) -> Self {
        match (left.value, right.value) {
            (None, _) => right,
            (_, None) => left,
            (Some(left_value), Some(right_value)) if left_value > right_value => left,
            (Some(left_value), Some(right_value)) if left_value < right_value => right,
            (Some(value), Some(_)) => Self::finite(value, left.inclusive && right.inclusive),
        }
    }

    fn min_upper(left: Self, right: Self) -> Self {
        match (left.value, right.value) {
            (None, _) => right,
            (_, None) => left,
            (Some(left_value), Some(right_value)) if left_value < right_value => left,
            (Some(left_value), Some(right_value)) if left_value > right_value => right,
            (Some(value), Some(_)) => Self::finite(value, left.inclusive && right.inclusive),
        }
    }
}

fn can_data_value_to_f64(value: &CanDataValue) -> anyhow::Result<f64> {
    match value {
        CanDataValue::Float32(value) => Ok(*value as f64),
        CanDataValue::Int32(value) => Ok(*value as f64),
        CanDataValue::Int16(value) => Ok(*value as f64),
        CanDataValue::Int8(value) => Ok(*value as f64),
        CanDataValue::UInt32(value) => Ok(*value as f64),
        CanDataValue::UInt16(value) => Ok(*value as f64),
        CanDataValue::UInt8(value) => Ok(*value as f64),
        CanDataValue::Boolean(value) => Ok(if *value { 1.0 } else { 0.0 }),
        CanDataValue::Raw(_) => bail!("raw CAN data must be decoded before applying a mapping"),
    }
}

/// Converts a mapped numeric value back into a typed CAN payload value.
fn can_data_value_from_f64(value: f64, data_type: CanDataType) -> anyhow::Result<CanDataValue> {
    ensure!(value.is_finite(), "raw value must be finite");

    match data_type {
        CanDataType::Float32 => Ok(CanDataValue::Float32(value as f32)),
        CanDataType::Int32 => Ok(CanDataValue::Int32(checked_integer::<i32>(value)?)),
        CanDataType::Int16 => Ok(CanDataValue::Int16(checked_integer::<i16>(value)?)),
        CanDataType::Int8 => Ok(CanDataValue::Int8(checked_integer::<i8>(value)?)),
        CanDataType::UInt32 => Ok(CanDataValue::UInt32(checked_integer::<u32>(value)?)),
        CanDataType::UInt16 => Ok(CanDataValue::UInt16(checked_integer::<u16>(value)?)),
        CanDataType::UInt8 => Ok(CanDataValue::UInt8(checked_integer::<u8>(value)?)),
        CanDataType::Boolean => {
            if (value - 0.0).abs() < f64::EPSILON {
                Ok(CanDataValue::Boolean(false))
            } else if (value - 1.0).abs() < f64::EPSILON {
                Ok(CanDataValue::Boolean(true))
            } else {
                bail!("boolean raw values must map back to 0 or 1, got {value}")
            }
        }
    }
}

/// Checks that a floating-point inverse-mapped value can be represented as an integer CAN type.
fn checked_integer<T>(value: f64) -> anyhow::Result<T>
where
    T: TryFrom<i128>,
    <T as TryFrom<i128>>::Error: std::fmt::Debug,
{
    let rounded = value.round();

    T::try_from(rounded as i128).map_err(|_| anyhow::anyhow!("raw value {rounded} is out of range"))
}

#[cfg(test)]
mod tests {
    use liquidcan::payloads::{CanDataType, CanDataValue};
    use toml::Value;

    use super::{LogicalValue, NodeMapping};

    #[test]
    fn parses_and_applies_mapping_schema() {
        let mapping = NodeMapping::parse_mapping(
            r##"
id = "example"

[[mapping]]
name = "tank_pressure"
type = "telemetry"
raw = { node = "ECU", field = "pressure_adc" }
value = { slope = 0.5, offset = 1.0, unit = "bar" }

[[mapping.logical]]
range = { min = 100 }
value = "High"
color = "#ff0000"

[[mapping.logical]]
range = { max = 100 }
value = "Normal"
"##,
        )
        .expect("mapping should parse");

        let entry = mapping
            .get_mapping_for_name("tank_pressure")
            .expect("entry should exist");

        let mapped = entry
            .mapped_value(&CanDataValue::UInt16(198))
            .expect("raw value should map");
        assert_eq!(mapped.value, 100.0);
        assert_eq!(mapped.unit, "bar");

        assert_eq!(
            entry.logical_value(mapped.value),
            Some(LogicalValue {
                value: Value::String("High".to_string()),
                color: Some("#ff0000".to_string()),
            })
        );
    }

    #[test]
    fn rejects_duplicate_mapping_names() {
        let error = NodeMapping::parse_mapping(
            r#"
[[mapping]]
name = "duplicate"
raw = { node = "node1", field = "field1" }
type = "telemetry"
value = { slope = 1.0, offset = 0.0 }

[[mapping]]
name = "duplicate"
raw = { node = "node1", field = "field2" }
type = "telemetry"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect_err("duplicate names should fail validation");

        assert!(format!("{error:#}").contains("Duplicate mapping name"));
    }

    #[test]
    fn converts_mapped_value_back_to_raw_parameter_type() {
        let mapping = NodeMapping::parse_mapping(
            r#"
[[mapping]]
name = "valve_opening"
type = "parameter"
raw = { node = "ECU", field = "valve_raw" }
value = { slope = 0.5, offset = 10.0, unit = "%" }
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("valve_opening").unwrap();
        let raw = entry
            .raw_value_from_mapped(60.0, CanDataType::UInt8)
            .expect("mapped value should invert to raw");

        assert_eq!(raw, CanDataValue::UInt8(100));
    }

    #[test]
    fn checked_in_example_mapping_is_valid() {
        NodeMapping::load_mapping_from_file("tests/mapping/example1.toml")
            .expect("example mapping should be valid");
    }

    #[test]
    fn rejects_non_exhaustive_logical_rules() {
        let error = NodeMapping::parse_mapping(
            r#"
[[mapping]]
name = "temperature"
type = "telemetry"
raw = { node = "ECU", field = "temperature" }

[[mapping.logical]]
range = { max = 10 }
value = "Cold"

[[mapping.logical]]
range = { min = 10, min_inclusive = false }
value = "Hot"
"#,
        )
        .expect_err("rules should miss exactly 10");

        assert!(format!("{error:#}").contains("not exhaustive"));
    }

    #[test]
    fn rejects_overlapping_logical_rules() {
        let error = NodeMapping::parse_mapping(
            r#"
[[mapping]]
name = "temperature"
type = "telemetry"
raw = { node = "ECU", field = "temperature" }

[[mapping.logical]]
range = { max = 100 }
value = "Low"

[[mapping.logical]]
range = { max = 50 }
value = "Very low"

[[mapping.logical]]
range = { min = 100 }
value = "High"
"#,
        )
        .expect_err("second rule should overlap with the first");

        assert!(format!("{error:#}").contains("overlaps"));
    }

    #[test]
    fn accepts_adjacent_ranges() {
        NodeMapping::parse_mapping(
            r#"
[[mapping]]
name = "temperature"
type = "telemetry"
raw = { node = "ECU", field = "temperature" }

[[mapping.logical]]
range = { max = 10 }
value = "Cold"

[[mapping.logical]]
range = { min = 10 }
value = "Hot"
"#,
        )
        .expect("adjacent ranges should cover the threshold exactly once");
    }
}
