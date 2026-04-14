// crates/wash-runtime/src/plugin/config.rs
//! Shared config resolution helpers for host plugins.
//!
//! Provides a standard pattern for resolving config fields with priority:
//! Wasm dynamic value > static interface config fallback.

use std::collections::HashMap;

/// Resolve a required config field.
///
/// Wasm dynamic value takes priority, then falls back to interface static config.
/// Returns an error if neither source has the key.
pub fn resolve_field(
    wasm_value: Option<String>,
    interface_config: &HashMap<String, String>,
    key: &str,
) -> anyhow::Result<String> {
    if let Some(val) = wasm_value {
        return Ok(val);
    }
    interface_config
        .get(key)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required config field: {key}"))
}

/// Resolve an optional config field.
///
/// Wasm dynamic value takes priority, then falls back to interface static config.
/// Returns `None` if neither source has the key.
pub fn resolve_optional_field(
    wasm_value: Option<String>,
    interface_config: &HashMap<String, String>,
    key: &str,
) -> Option<String> {
    if wasm_value.is_some() {
        return wasm_value;
    }
    interface_config.get(key).cloned()
}
