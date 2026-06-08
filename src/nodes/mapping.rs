use anyhow::{Context, bail, ensure};
use liquidcan::payloads::{CanDataType, CanDataValue};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    fs,
    path::Path,
};
use toml::Value;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mapping {
    pub mapping: BTreeMap<String, Vec<MappingEntry>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingEntry {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub raw_field: String,
    #[serde(default)]
    pub value: ValueParams,
    #[serde(default)]
    pub logical: Vec<LogicalRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct LogicalRule {
    pub range: LogicalRange,
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogicalRange {
    /// Inclusive lower bound by default. If omitted, the range is unbounded below.
    #[serde(default = "default_unbounded_min")]
    pub min: f64,
    /// Exclusive upper bound by default. If omitted, the range is unbounded above.
    #[serde(default = "default_unbounded_max")]
    pub max: f64,
    #[serde(default = "default_min_inclusive")]
    pub min_inclusive: bool,
    #[serde(default)]
    pub max_inclusive: bool,
}

fn default_unbounded_min() -> f64 {
    f64::NEG_INFINITY
}

fn default_unbounded_max() -> f64 {
    f64::INFINITY
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct MappedValue {
    pub value: f64,
    pub unit: String,
}

pub struct MappingLookupResult<'a> {
    pub node_name: &'a str,
    pub mapping_entry: &'a MappingEntry,
}

impl Mapping {
    pub fn load_mapping_from_file(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::default());
        }

        Self::load_mapping_file(Path::new(path))
    }

    pub fn load_mapping_from_path(path: &str) -> anyhow::Result<Self> {
        if path.is_empty() {
            return Ok(Self::default());
        }

        Self::load_mapping_directory(Path::new(path))
    }

    pub fn parse_mapping(toml_str: &str) -> anyhow::Result<Self> {
        let config = toml::from_str::<Mapping>(toml_str)
            .map_err(|err| anyhow::anyhow!("Failed to parse mapping config: {}", err))?;

        config.validate()?;

        Ok(config)
    }

    fn load_mapping_file(path: &Path) -> anyhow::Result<Self> {
        let toml_str = fs::read_to_string(path)
            .with_context(|| format!("Failed to read mapping config file at {}", path.display()))?;

        Self::parse_mapping(&toml_str)
            .with_context(|| format!("Failed to load mapping config from {}", path.display()))
    }

    fn load_mapping_directory(path: &Path) -> anyhow::Result<Self> {
        let mut entries = fs::read_dir(path)
            .with_context(|| format!("Failed to read mapping directory {}", path.display()))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("Failed to list mapping directory {}", path.display()))?;

        entries.retain(|entry| {
            entry.is_file()
                && entry
                    .extension()
                    .is_some_and(|extension| extension.eq_ignore_ascii_case("toml"))
        });
        entries.sort();

        ensure!(
            !entries.is_empty(),
            "mapping directory {} contains no TOML files",
            path.display()
        );

        let mut combined = Self::default();
        for entry in entries {
            let mapping = Self::load_mapping_file(&entry).with_context(|| {
                format!("Failed to load mapping config from {}", entry.display())
            })?;

            for (node, fields) in mapping.mapping {
                combined.mapping.entry(node).or_default().extend(fields);
            }
        }

        combined.validate().with_context(|| {
            format!("Mapping validation failed for directory {}", path.display())
        })?;

        Ok(combined)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut names = HashSet::new();
        let mut raw_fields = HashSet::new();
        for (node, fields) in &self.mapping {
            ensure!(
                !node.trim().is_empty(),
                "mapping contains an entry with an empty node name"
            );
            for field in fields {
                field.validate().with_context(|| {
                    format!("mapping for node {} field {} is invalid", node, field.name)
                })?;

                let raw_id = (node.as_str(), field.raw_field.as_str());
                if !raw_fields.insert(raw_id) {
                    anyhow::bail!(
                        "Duplicate raw field mapping for node '{}' field '{}'",
                        node,
                        field.raw_field
                    );
                }

                if !names.insert(field.name.as_str()) {
                    anyhow::bail!("Duplicate mapping name '{}'", field.name);
                }
            }
        }

        Ok(())
    }

    pub fn get_mapping_for_name(&self, name: &str) -> Option<MappingLookupResult<'_>> {
        self.mapping.iter().find_map(|(node, fields)| {
            fields
                .iter()
                .find(|field| field.name == name)
                .map(|field| MappingLookupResult {
                    node_name: node.as_str(),
                    mapping_entry: field,
                })
        })
    }

    pub fn get_mapping_for_raw(&self, node: &str, field: &str) -> Option<MappingLookupResult<'_>> {
        self.mapping
            .get_key_value(node)
            .and_then(|(node, mapping_entries)| {
                mapping_entries
                    .iter()
                    .find(|mapping| mapping.raw_field == field)
                    .map(|mapping| MappingLookupResult {
                        node_name: node,
                        mapping_entry: mapping,
                    })
            })
    }
}

impl MappingEntry {
    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.name.trim().is_empty(),
            "mapping name must be non-empty",
        );
        ensure!(
            !self.name.contains(':'),
            "mapping name '{}' cannot contain colons, which are reserved characters to differentiate raw names from mapped names",
            self.name
        );

        ensure!(
            !self.raw_field.trim().is_empty(),
            "mapping {} has an empty raw_field",
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
    /// Empty logical rules are allowed. Ranges must be non-empty and non-overlapping.
    fn validate_logical_rules(&self) -> anyhow::Result<()> {
        if self.logical.is_empty() {
            return Ok(());
        }

        let mut covered_ranges = Vec::new();

        for (index, rule) in self.logical.iter().enumerate() {
            if !rule.range.is_non_empty() {
                bail!(
                    "Logical rule {} for mapping {} has an empty range {}",
                    index + 1,
                    self.name,
                    rule.range.describe()
                );
            }

            for (covered_index, covered_range) in covered_ranges.iter().enumerate() {
                if let Some(overlap) = rule.range.intersection(covered_range) {
                    bail!(
                        "Logical rule {} for mapping {} overlaps with rule {} in {}; overlapping ranges are ambiguous",
                        index + 1,
                        self.name,
                        covered_index + 1,
                        overlap.describe()
                    );
                }
            }

            covered_ranges.push(rule.range.clone());
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
            })
    }
}

impl LogicalRule {
    fn matches(&self, mapped_value: f64) -> bool {
        self.range.contains(mapped_value)
    }
}

impl LogicalRange {
    fn contains(&self, value: f64) -> bool {
        let above_lower = if self.min_inclusive {
            value >= self.min
        } else {
            value > self.min
        };
        let below_upper = if self.max_inclusive {
            value <= self.max
        } else {
            value < self.max
        };
        above_lower && below_upper
    }

    fn intersection(&self, other: &Self) -> Option<Self> {
        let max_cmp = self.max.partial_cmp(&other.max).unwrap();
        let min_cmp = self.min.partial_cmp(&other.min).unwrap();

        let max = self.max.min(other.max);
        let min = self.min.max(other.min);

        let min_inclusive = match min_cmp {
            std::cmp::Ordering::Less => other.min_inclusive,
            std::cmp::Ordering::Greater => self.min_inclusive,
            std::cmp::Ordering::Equal => self.min_inclusive && other.min_inclusive,
        };

        let max_inclusive = match max_cmp {
            std::cmp::Ordering::Less => self.max_inclusive,
            std::cmp::Ordering::Greater => other.max_inclusive,
            std::cmp::Ordering::Equal => self.max_inclusive && other.max_inclusive,
        };

        let intersection = Self {
            min,
            max,
            min_inclusive,
            max_inclusive,
        };
        if intersection.is_non_empty() {
            Some(intersection)
        } else {
            None
        }
    }

    fn is_non_empty(&self) -> bool {
        if self.max > self.min {
            return true;
        }

        if self.max == self.min {
            return self.min_inclusive && self.max_inclusive;
        }

        false
    }

    fn describe(&self) -> String {
        format!(
            "{}{}, {}{}",
            if self.min_inclusive { "[" } else { "(" },
            self.min,
            self.max,
            if self.max_inclusive { "]" } else { ")" }
        )
    }
}

pub fn can_data_value_to_f64(value: &CanDataValue) -> anyhow::Result<f64> {
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
pub fn can_data_value_from_f64(value: f64, data_type: CanDataType) -> anyhow::Result<CanDataValue> {
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
    ensure!(
        (value - rounded).abs() <= 1e-9,
        "raw value {value} is not an integer"
    );

    T::try_from(rounded as i128).map_err(|_| anyhow::anyhow!("raw value {rounded} is out of range"))
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use chrono::Utc;
    use liquidcan::payloads::{CanDataType, CanDataValue};
    use toml::Value;

    use super::{LogicalValue, Mapping};

    #[test]
    fn parses_and_applies_mapping_schema() {
        let mapping = Mapping::parse_mapping(
            r##"
[[mapping.ECU]]
name = "tank_pressure"
type = "telemetry"
raw_field = "pressure_adc"
value = { slope = 0.5, offset = 1.0, unit = "bar" }

[[mapping.ECU.logical]]
range = { min = 100 }
value = "High"

[[mapping.ECU.logical]]
range = { max = 100 }
value = "Normal"
"##,
        )
        .expect("mapping should parse");

        let lookup = mapping
            .get_mapping_for_name("tank_pressure")
            .expect("entry should exist");

        let mapped = lookup
            .mapping_entry
            .mapped_value(&CanDataValue::UInt16(198))
            .expect("raw value should map");
        assert_eq!(mapped.value, 100.0);
        assert_eq!(mapped.unit, "bar");

        assert_eq!(
            lookup.mapping_entry.logical_value(mapped.value),
            Some(LogicalValue {
                value: Value::String("High".to_string()),
            })
        );
    }

    #[test]
    fn rejects_empty_node_name() {
        let error = Mapping::parse_mapping(
            r#"
[mapping]
"   " = [{ name = "x", type = "telemetry", raw_field = "f" }]
"#,
        )
        .expect_err("empty node name should fail validation");

        assert!(format!("{error:#}").contains("empty node name"));
    }

    #[test]
    fn rejects_empty_mapping_name() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = ""
type = "telemetry"
raw_field = "field"
"#,
        )
        .expect_err("empty mapping name should fail validation");

        assert!(format!("{error:#}").contains("mapping name must be non-empty"));
    }

    #[test]
    fn rejects_empty_raw_field() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = ""
"#,
        )
        .expect_err("empty raw_field should fail validation");

        assert!(format!("{error:#}").contains("has an empty raw_field"));
    }

    #[test]
    fn rejects_zero_slope() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"
value = { slope = 0.0, offset = 0.0 }
"#,
        )
        .expect_err("zero slope should fail validation");

        assert!(format!("{error:#}").contains("slope of zero"));
    }

    #[test]
    fn rejects_non_finite_slope() {
        let mapping = Mapping {
            mapping: [(
                "ECU".to_string(),
                vec![super::MappingEntry {
                    name: "x".to_string(),
                    field_type: super::FieldType::Telemetry,
                    raw_field: "f".to_string(),
                    value: super::ValueParams {
                        slope: f64::INFINITY,
                        offset: 0.0,
                        unit: "".to_string(),
                    },
                    logical: vec![],
                }],
            )]
            .into_iter()
            .collect(),
        };

        let error = mapping
            .validate()
            .expect_err("non-finite slope should fail validation");
        assert!(format!("{error:#}").contains("non-finite slope"));
    }

    #[test]
    fn rejects_non_finite_offset() {
        let mapping = Mapping {
            mapping: [(
                "ECU".to_string(),
                vec![super::MappingEntry {
                    name: "x".to_string(),
                    field_type: super::FieldType::Telemetry,
                    raw_field: "f".to_string(),
                    value: super::ValueParams {
                        slope: 1.0,
                        offset: f64::NAN,
                        unit: "".to_string(),
                    },
                    logical: vec![],
                }],
            )]
            .into_iter()
            .collect(),
        };

        let error = mapping
            .validate()
            .expect_err("non-finite offset should fail validation");
        assert!(format!("{error:#}").contains("non-finite offset"));
    }

    #[test]
    fn rejects_duplicate_mapping_name_across_nodes() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.NodeA]]
name = "dup"
type = "telemetry"
raw_field = "a"

[[mapping.NodeB]]
name = "dup"
type = "telemetry"
raw_field = "b"
"#,
        )
        .expect_err("duplicate mapping names across nodes should fail validation");

        assert!(format!("{error:#}").contains("Duplicate mapping name"));
    }

    #[test]
    fn looks_up_mapping_by_raw_field_and_type() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "tank_pressure"
type = "telemetry"
raw_field = "pressure_adc"

[[mapping.ECU]]
name = "valve_opening"
type = "parameter"
raw_field = "valve_raw"
"#,
        )
        .expect("mapping should parse");

        let telemetry = mapping
            .get_mapping_for_raw("ECU", "pressure_adc")
            .expect("telemetry mapping should exist");
        assert_eq!(telemetry.mapping_entry.name, "tank_pressure");

        let parameter = mapping
            .get_mapping_for_raw("ECU", "valve_raw")
            .expect("parameter mapping should exist");
        assert_eq!(parameter.mapping_entry.name, "valve_opening");
    }

    #[test]
    fn logical_value_returns_none_when_value_in_gap() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"

[[mapping.ECU.logical]]
range = { min = 0, max = 10 }
value = "Low"

[[mapping.ECU.logical]]
range = { min = 20, max = 30 }
value = "High"
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        assert_eq!(entry.mapping_entry.logical_value(15.0), None);
    }

    #[test]
    fn logical_range_inclusive_exclusive_boundaries() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"

[[mapping.ECU.logical]]
range = { max = 10 }
value = "Low"

[[mapping.ECU.logical]]
range = { min = 10 }
value = "High"
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        assert_eq!(
            entry.mapping_entry.logical_value(10.0),
            Some(LogicalValue {
                value: Value::String("High".to_string())
            })
        );
    }

    #[test]
    fn rejects_overlap_due_to_inclusive_boundary() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"

[[mapping.ECU.logical]]
range = { max = 10, max_inclusive = true }
value = "Low"

[[mapping.ECU.logical]]
range = { min = 10, min_inclusive = true }
value = "High"
"#,
        )
        .expect_err("inclusive boundary overlap should fail validation");

        assert!(format!("{error:#}").contains("overlaps"));
    }

    #[test]
    fn accepts_single_point_range_when_both_inclusive() {
        Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"

[[mapping.ECU.logical]]
range = { min = 10, max = 10, min_inclusive = true, max_inclusive = true }
value = "Ten"
"#,
        )
        .expect("single-point inclusive range should be valid");
    }

    #[test]
    fn rejects_single_point_range_when_any_exclusive() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"

[[mapping.ECU.logical]]
range = { min = 10, max = 10, min_inclusive = true, max_inclusive = false }
value = "Ten"
"#,
        )
        .expect_err("single-point exclusive range should be rejected");

        assert!(format!("{error:#}").contains("empty range"));
    }

    #[test]
    fn mapped_value_converts_all_can_types_to_f64() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Int8(-5))
            .unwrap();
        assert!((mapped.value - (-5.0)).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Int16(-300))
            .unwrap();
        assert!((mapped.value - (-300.0)).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Int32(-100_000))
            .unwrap();
        assert!((mapped.value - (-100_000.0)).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::UInt8(250))
            .unwrap();
        assert!((mapped.value - 250.0).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::UInt16(50_000))
            .unwrap();
        assert!((mapped.value - 50_000.0).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::UInt32(1_000_000))
            .unwrap();
        assert!((mapped.value - 1_000_000.0).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Float32(1.5))
            .unwrap();
        assert!((mapped.value - 1.5).abs() < 1e-6);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Boolean(true))
            .unwrap();
        assert!((mapped.value - 1.0).abs() < 1e-12);

        let mapped = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Boolean(false))
            .unwrap();
        assert!((mapped.value - 0.0).abs() < 1e-12);
    }

    #[test]
    fn mapped_value_rejects_raw() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "telemetry"
raw_field = "f"
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        let error = entry
            .mapping_entry
            .mapped_value(&CanDataValue::Raw(vec![1, 2, 3]))
            .expect_err("raw values should be rejected");

        assert!(
            format!("{error:#}").contains("raw CAN data must be decoded before applying a mapping")
        );
    }

    #[test]
    fn raw_value_from_mapped_rejects_out_of_range_integers() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "parameter"
raw_field = "f"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        let error = entry
            .mapping_entry
            .raw_value_from_mapped(9999.0, CanDataType::UInt8)
            .expect_err("out of range values should fail");
        assert!(format!("{error:#}").contains("out of range"));
    }

    #[test]
    fn raw_value_from_mapped_boolean_accepts_only_0_or_1() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "parameter"
raw_field = "f"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        assert_eq!(
            entry
                .mapping_entry
                .raw_value_from_mapped(0.0, CanDataType::Boolean)
                .unwrap(),
            CanDataValue::Boolean(false)
        );
        assert_eq!(
            entry
                .mapping_entry
                .raw_value_from_mapped(1.0, CanDataType::Boolean)
                .unwrap(),
            CanDataValue::Boolean(true)
        );

        let error = entry
            .mapping_entry
            .raw_value_from_mapped(0.5, CanDataType::Boolean)
            .expect_err("non 0/1 boolean values should fail");
        assert!(format!("{error:#}").contains("must map back to 0 or 1"));
    }

    #[test]
    fn raw_value_from_mapped_rejects_nan_or_inf() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "x"
type = "parameter"
raw_field = "f"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect("mapping should parse");

        let entry = mapping.get_mapping_for_name("x").unwrap();
        let error = entry
            .mapping_entry
            .raw_value_from_mapped(f64::NAN, CanDataType::UInt8)
            .expect_err("NaN should fail");
        assert!(format!("{error:#}").contains("must be finite"));

        let error = entry
            .mapping_entry
            .raw_value_from_mapped(f64::INFINITY, CanDataType::UInt8)
            .expect_err("Inf should fail");
        assert!(format!("{error:#}").contains("must be finite"));
    }

    #[test]
    fn load_mapping_from_file_empty_path_returns_default() {
        let mapping = Mapping::load_mapping_from_file("").unwrap();
        assert!(mapping.mapping.is_empty());
    }

    #[test]
    fn load_mapping_from_path_empty_path_returns_default() {
        let mapping = Mapping::load_mapping_from_path("").unwrap();
        assert!(mapping.mapping.is_empty());
    }

    #[test]
    fn load_mapping_from_file_missing_file_errors_with_context() {
        let path = std::env::temp_dir().join(format!(
            "ferro_flow_mapping_missing_{}_{}.toml",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));

        let error = Mapping::load_mapping_from_file(path.to_str().unwrap())
            .expect_err("missing mapping file should error");
        assert!(format!("{error:#}").contains("Failed to read mapping config file"));
    }

    #[test]
    fn load_mapping_from_path_missing_dir_errors_with_context() {
        let path = std::env::temp_dir().join(format!(
            "ferro_flow_mapping_missing_dir_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));

        let error = Mapping::load_mapping_from_path(path.to_str().unwrap())
            .expect_err("missing mapping directory should error");
        assert!(format!("{error:#}").contains("Failed to read mapping directory"));
    }

    #[test]
    fn rejects_duplicate_mapping_names() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.node1]]
name = "duplicate"
raw_field = "field1"
type = "telemetry"
value = { slope = 1.0, offset = 0.0 }

[[mapping.node1]]
name = "duplicate"
raw_field = "field2"
type = "telemetry"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect_err("duplicate names should fail validation");

        assert!(format!("{error:#}").contains("Duplicate mapping name"));
    }

    #[test]
    fn converts_mapped_value_back_to_raw_parameter_type() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "valve_opening"
type = "parameter"
raw_field = "valve_raw"
value = { slope = 0.5, offset = 10.0, unit = "%" }
"#,
        )
        .expect("mapping should parse");

        let lookup = mapping.get_mapping_for_name("valve_opening").unwrap();
        let raw = lookup
            .mapping_entry
            .raw_value_from_mapped(60.0, CanDataType::UInt8)
            .expect("mapped value should invert to raw");

        assert_eq!(raw, CanDataValue::UInt8(100));
    }

    #[test]
    fn rejects_fractional_raw_values_for_integer_parameters() {
        let mapping = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "valve_opening"
type = "parameter"
raw_field = "valve_raw"
value = { slope = 1.0, offset = 0.0 }
"#,
        )
        .expect("mapping should parse");

        let lookup = mapping.get_mapping_for_name("valve_opening").unwrap();
        let error = lookup
            .mapping_entry
            .raw_value_from_mapped(10.2, CanDataType::UInt8)
            .expect_err("fractional integer raw values should fail");

        assert!(format!("{error:#}").contains("is not an integer"));
    }

    #[test]
    fn rejects_duplicate_raw_fields_across_mapping_files() {
        let dir = temp_mapping_dir("duplicate_raw");
        fs::write(
            dir.join("a.toml"),
            r#"
[[mapping.ECU]]
name = "first"
type = "telemetry"
raw_field = "pressure"
"#,
        )
        .unwrap();
        fs::write(
            dir.join("b.toml"),
            r#"
[[mapping.ECU]]
name = "second"
type = "telemetry"
raw_field = "pressure"
"#,
        )
        .unwrap();

        let error = Mapping::load_mapping_from_path(dir.to_str().unwrap())
            .expect_err("duplicate raw fields across files should fail");

        assert!(format!("{error:#}").contains("Duplicate raw field"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn loads_mapping_directory() {
        let mapping = Mapping::load_mapping_from_path("tests/mapping/split")
            .expect("split mapping directory should be valid");

        assert!(mapping.get_mapping_for_name("fuel_level").is_some());
        assert!(mapping.get_mapping_for_name("throttle_state").is_some());
    }

    #[test]
    fn rejects_empty_mapping_directory() {
        let dir = temp_mapping_dir("empty");

        let error = Mapping::load_mapping_from_path(dir.to_str().unwrap())
            .expect_err("empty mapping directories should fail");

        assert!(format!("{error:#}").contains("contains no TOML files"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn checked_in_example_mapping_is_valid() {
        Mapping::load_mapping_from_file("tests/mapping/example1.toml")
            .expect("example mapping should be valid");
    }

    #[test]
    fn rejects_overlapping_logical_rules() {
        let error = Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "temperature"
type = "telemetry"
raw_field = "temperature"

[[mapping.ECU.logical]]
range = { max = 100 }
value = "Low"

[[mapping.ECU.logical]]
range = { max = 50 }
value = "Very low"

[[mapping.ECU.logical]]
range = { min = 100 }
value = "High"
"#,
        )
        .expect_err("second rule should overlap with the first");

        assert!(format!("{error:#}").contains("overlaps"));
    }

    #[test]
    fn accepts_adjacent_ranges() {
        Mapping::parse_mapping(
            r#"
[[mapping.ECU]]
name = "temperature"
type = "telemetry"
raw_field = "temperature"

[[mapping.ECU.logical]]
range = { max = 10 }
value = "Cold"

[[mapping.ECU.logical]]
range = { min = 10 }
value = "Hot"
"#,
        )
        .expect("adjacent ranges should cover the threshold exactly once");
    }

    fn temp_mapping_dir(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("ferro_flow_mapping_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }
}
