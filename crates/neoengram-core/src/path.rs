use std::{collections::BTreeSet, fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

use crate::{ValidationError, ValidationErrorKind, ValidationResult};

const REPOSITORY_DIRECTORY: &str = ".neoengram";
const STORAGE_TEMP_PREFIX: &str = ".neoengram-tmp-";

/// A single normalized, portable UTF-8 component of a logical repository path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PathComponent(String);

impl PathComponent {
    /// Parses a component, rejecting non-NFC and cross-platform unsafe names.
    pub fn parse(value: impl Into<String>) -> ValidationResult<Self> {
        let value = value.into();
        validate_component(&value)?;
        Ok(Self(value))
    }

    /// Returns the normalized component text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the case-folded key used for portable collision checks.
    pub fn portable_key(&self) -> String {
        portable_component_key(&self.0)
    }
}

impl fmt::Display for PathComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PathComponent {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for PathComponent {
    type Error = ValidationError;

    fn try_from(value: String) -> ValidationResult<Self> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for PathComponent {
    type Error = ValidationError;

    fn try_from(value: &str) -> ValidationResult<Self> {
        Self::parse(value)
    }
}

impl AsRef<str> for PathComponent {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Serialize for PathComponent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PathComponent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

/// A non-empty, repository-relative UTF-8 path with `/` separators.
///
/// Construction rejects absolute paths, dot components, non-NFC text, NeoEngram internal names,
/// Windows device names, and characters that are not portable across supported platforms.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalPath(String);

impl LogicalPath {
    /// Parses and validates a complete logical path.
    pub fn parse(value: impl Into<String>) -> ValidationResult<Self> {
        let value = value.into();
        if value.is_empty() {
            return Err(invalid_path("logical path must not be empty"));
        }
        if value.starts_with('/') {
            return Err(invalid_path(format!(
                "logical path must be repository-relative: {value}"
            )));
        }
        if value.contains('\\') {
            return Err(invalid_path(format!(
                "logical path must use '/' separators: {value}"
            )));
        }
        for component in value.split('/') {
            validate_component(component).map_err(|error| {
                invalid_path(format!("invalid logical path {value}: {}", error.message()))
            })?;
        }
        Ok(Self(value))
    }

    /// Returns the canonical repository-relative path text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterates over validated path components without allocating.
    pub fn components(&self) -> impl DoubleEndedIterator<Item = &str> {
        self.0.split('/')
    }

    /// Returns the final path component.
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(self.as_str())
    }

    /// Returns the parent logical path, or `None` for a root-level entry.
    pub fn parent(&self) -> Option<Self> {
        self.0
            .rsplit_once('/')
            .map(|(parent, _)| Self(parent.to_owned()))
    }

    /// Appends one validated component.
    pub fn join(&self, component: &PathComponent) -> Self {
        Self(format!("{}/{}", self.0, component.as_str()))
    }

    /// Returns the normalized case-insensitive key used for portable collision checks.
    pub fn portable_key(&self) -> String {
        self.components()
            .map(portable_component_key)
            .collect::<Vec<_>>()
            .join("/")
    }

    /// Reports whether this path is a strict logical ancestor of `other`.
    pub fn is_ancestor_of(&self, other: &Self) -> bool {
        other
            .as_str()
            .strip_prefix(self.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

impl fmt::Display for LogicalPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for LogicalPath {
    type Err = ValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for LogicalPath {
    type Error = ValidationError;

    fn try_from(value: String) -> ValidationResult<Self> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for LogicalPath {
    type Error = ValidationError;

    fn try_from(value: &str) -> ValidationResult<Self> {
        Self::parse(value)
    }
}

impl AsRef<str> for LogicalPath {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Serialize for LogicalPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LogicalPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

/// Validates that paths are strictly sorted, portable-unique, and free of file/prefix conflicts.
pub fn validate_path_set<'a>(
    paths: impl IntoIterator<Item = &'a LogicalPath>,
) -> ValidationResult<()> {
    let mut previous: Option<&LogicalPath> = None;
    let mut portable_paths: BTreeSet<String> = BTreeSet::new();
    for path in paths {
        if previous.is_some_and(|previous| previous >= path) {
            return Err(invalid_path(format!(
                "logical paths must be strictly sorted and unique: {path}"
            )));
        }
        previous = Some(path);

        let portable_path = path.portable_key();
        if portable_paths.contains(&portable_path) {
            return Err(invalid_path(format!(
                "logical paths collide under portable case rules: {path}"
            )));
        }
        for (offset, _) in portable_path.match_indices('/') {
            if portable_paths.contains(&portable_path[..offset]) {
                return Err(invalid_path(format!(
                    "a file path is also an ancestor of another path: {path}"
                )));
            }
        }
        let descendant_prefix = format!("{portable_path}/");
        if portable_paths
            .range(descendant_prefix.clone()..)
            .next()
            .is_some_and(|candidate| candidate.starts_with(&descendant_prefix))
        {
            return Err(invalid_path(format!(
                "a file path is also an ancestor of another path: {path}"
            )));
        }
        portable_paths.insert(portable_path);
    }
    Ok(())
}

fn validate_component(component: &str) -> ValidationResult<()> {
    if component.is_empty() || component == "." || component == ".." {
        return Err(invalid_path(
            "path component must not be empty, '.' or '..'",
        ));
    }
    if component.contains(['/', '\\', '\0']) {
        return Err(invalid_path(format!(
            "path component contains a separator or NUL: {component}"
        )));
    }
    if !component.nfc().eq(component.chars()) {
        return Err(invalid_path(format!(
            "path component must use Unicode NFC: {component}"
        )));
    }
    if component.ends_with([' ', '.']) {
        return Err(invalid_path(format!(
            "path component must not end with a space or period: {component}"
        )));
    }
    if component.chars().any(|character| {
        character <= '\u{1f}' || matches!(character, '<' | '>' | ':' | '"' | '|' | '?' | '*')
    }) {
        return Err(invalid_path(format!(
            "path component contains a non-portable character: {component}"
        )));
    }
    if component.eq_ignore_ascii_case(REPOSITORY_DIRECTORY)
        || component
            .as_bytes()
            .get(..STORAGE_TEMP_PREFIX.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(STORAGE_TEMP_PREFIX.as_bytes()))
    {
        return Err(invalid_path(format!(
            "path component uses a reserved NeoEngram name: {component}"
        )));
    }

    let device_name = component
        .split('.')
        .next()
        .unwrap_or(component)
        .to_ascii_uppercase();
    let reserved = matches!(
        device_name.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
    ) || device_name
        .strip_prefix("COM")
        .is_some_and(is_reserved_device_number)
        || device_name
            .strip_prefix("LPT")
            .is_some_and(is_reserved_device_number);
    if reserved {
        return Err(invalid_path(format!(
            "path component uses a reserved Windows device name: {component}"
        )));
    }
    Ok(())
}

fn is_reserved_device_number(suffix: &str) -> bool {
    matches!(
        suffix,
        "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "\u{b9}" | "\u{b2}" | "\u{b3}"
    )
}

fn portable_component_key(component: &str) -> String {
    component.case_fold().nfc().collect()
}

fn invalid_path(message: impl Into<String>) -> ValidationError {
    ValidationError::new(ValidationErrorKind::InvalidPath, message)
}
