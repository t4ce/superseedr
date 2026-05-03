// SPDX-FileCopyrightText: 2026 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use serde::de::DeserializeOwned;
use serde::Serialize;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) const SCHEMA_VERSION: u32 = 1;

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_path_for(path: &Path) -> PathBuf {
    let counter = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut file_name = path
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("superseedr"));
    file_name.push(format!(".tmp.{}.{}", std::process::id(), counter));

    match path.parent() {
        Some(parent) => parent.join(file_name),
        None => PathBuf::from(file_name),
    }
}

pub(crate) fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp_path = temp_path_for(path);
    if let Err(error) = fs::write(&tmp_path, bytes) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }
    if let Err(error) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(error);
    }
    Ok(())
}

pub(crate) fn write_string_atomically(path: &Path, content: &str) -> io::Result<()> {
    write_bytes_atomically(path, content.as_bytes())
}

pub(crate) fn serialize_versioned_toml<T: Serialize>(value: &T) -> io::Result<String> {
    let mut toml_value = toml::Value::try_from(value).map_err(io::Error::other)?;
    let table = toml_value
        .as_table_mut()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Expected TOML table"))?;
    table.insert(
        "schema_version".to_string(),
        toml::Value::Integer(i64::from(SCHEMA_VERSION)),
    );
    toml::to_string_pretty(&toml_value).map_err(io::Error::other)
}

pub(crate) fn deserialize_versioned_toml<T: DeserializeOwned>(content: &str) -> io::Result<T> {
    let parsed: toml::Value = toml::from_str(content)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let Some(table) = parsed.as_table() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected TOML table",
        ));
    };

    if let Some(schema_version_value) = table.get("schema_version") {
        let Some(schema_version) = schema_version_value.as_integer() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema_version must be an integer",
            ));
        };
        if schema_version != i64::from(SCHEMA_VERSION) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported schema version {schema_version}"),
            ));
        }

        let mut stripped = table.clone();
        stripped.remove("schema_version");
        return toml::Value::Table(stripped)
            .try_into()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
    }

    toml::from_str(content).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

pub(crate) fn write_toml_atomically<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let content = serialize_versioned_toml(value)?;
    write_string_atomically(path, &content)
}

pub(crate) fn serialize_versioned_json<T: Serialize>(value: &T) -> io::Result<String> {
    let mut json_value = serde_json::to_value(value).map_err(io::Error::other)?;
    let object = json_value
        .as_object_mut()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Expected JSON object"))?;
    object.insert(
        "schema_version".to_string(),
        serde_json::Value::from(SCHEMA_VERSION),
    );
    serde_json::to_string_pretty(&json_value).map_err(io::Error::other)
}

pub(crate) fn deserialize_versioned_json<T: DeserializeOwned>(content: &str) -> io::Result<T> {
    let parsed: serde_json::Value = serde_json::from_str(content)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let Some(object) = parsed.as_object() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected JSON object",
        ));
    };

    if let Some(schema_version_value) = object.get("schema_version") {
        let Some(schema_version) = schema_version_value.as_u64() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "schema_version must be an unsigned integer",
            ));
        };
        if schema_version != u64::from(SCHEMA_VERSION) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported schema version {schema_version}"),
            ));
        }

        let mut stripped = object.clone();
        stripped.remove("schema_version");
        return serde_json::from_value(serde_json::Value::Object(stripped))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error));
    }

    serde_json::from_str(content).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

pub(crate) async fn write_bytes_atomically_async(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let tmp_path = temp_path_for(path);
    if let Err(error) = tokio::fs::write(&tmp_path, bytes).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(error);
    }
    if let Err(error) = tokio::fs::rename(&tmp_path, path).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_bytes_atomically_replaces_file_without_leaving_tmp() {
        let dir = tempdir().expect("create tempdir");
        let path = dir.path().join("sample.txt");

        write_bytes_atomically(&path, b"first").expect("write first");
        write_bytes_atomically(&path, b"second").expect("write second");

        assert_eq!(fs::read_to_string(&path).expect("read file"), "second");
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .expect("read temp dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "unexpected temp files: {leftovers:?}");
    }

    #[test]
    fn temp_paths_are_unique_for_same_target() {
        let dir = tempdir().expect("create tempdir");
        let path = dir.path().join("sample.txt");

        let first = temp_path_for(&path);
        let second = temp_path_for(&path);

        assert_ne!(first, second);
    }

    #[test]
    fn write_bytes_atomically_removes_tmp_when_rename_fails() {
        let dir = tempdir().expect("create tempdir");
        let path = dir.path().join("blocked-target");
        fs::create_dir(&path).expect("create blocking directory");

        let error = write_bytes_atomically(&path, b"new contents")
            .expect_err("rename over directory should fail");

        assert!(!error.to_string().is_empty());
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .expect("read temp dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "unexpected temp files: {leftovers:?}");
    }
}
