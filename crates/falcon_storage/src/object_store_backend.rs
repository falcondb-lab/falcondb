//! Object storage backend abstraction for PITR WAL archival.
//!
//! Backends:
//! - `LocalFsBackend` — local filesystem (default, always available)
//! - `S3Backend` — S3-compatible (AWS S3, MinIO, Ceph) via `object_store` crate (feature: pitr_object_store)
//! - `AzureBackend` — Azure Blob Storage via `object_store` crate (feature: pitr_object_store)
//!
//! Usage: configure in `falcon.toml` under `[pitr]`.

use std::path::{Path, PathBuf};

/// Opaque key within an object store (equivalent to a file path / S3 object key).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey(pub String);

impl ObjectKey {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Synchronous object storage operations needed for PITR archival.
///
/// All implementations must be Send + Sync.
pub trait ObjectStoreBackend: Send + Sync {
    /// Upload `data` as object `key`. Overwrites if exists.
    fn put(&self, key: &ObjectKey, data: &[u8]) -> Result<(), String>;

    /// Download object `key` into a Vec<u8>.
    fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, String>;

    /// Check whether `key` exists.
    fn exists(&self, key: &ObjectKey) -> Result<bool, String>;

    /// Delete object `key`. No-op if not found.
    fn delete(&self, key: &ObjectKey) -> Result<(), String>;

    /// List all keys with the given prefix.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectKey>, String>;

    /// Human-readable URI for display / observability.
    fn uri(&self) -> String;
}

// ── Local filesystem backend ─────────────────────────────────────────────────

/// Stores objects as files under `root_dir/<key>`.
#[derive(Debug, Clone)]
pub struct LocalFsBackend {
    root: PathBuf,
}

impl LocalFsBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn full_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(&key.0)
    }
}

impl ObjectStoreBackend for LocalFsBackend {
    fn put(&self, key: &ObjectKey, data: &[u8]) -> Result<(), String> {
        let path = self.full_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&path, data).map_err(|e| e.to_string())
    }

    fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, String> {
        std::fs::read(self.full_path(key)).map_err(|e| e.to_string())
    }

    fn exists(&self, key: &ObjectKey) -> Result<bool, String> {
        Ok(self.full_path(key).exists())
    }

    fn delete(&self, key: &ObjectKey) -> Result<(), String> {
        let path = self.full_path(key);
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectKey>, String> {
        let dir = self.root.join(prefix);
        let base = if dir.is_dir() {
            dir
        } else {
            dir.parent().unwrap_or(&self.root).to_path_buf()
        };

        if !base.exists() {
            return Ok(vec![]);
        }

        let mut keys = vec![];
        for entry in std::fs::read_dir(&base).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.is_file() {
                if let Ok(rel) = path.strip_prefix(&self.root) {
                    let key_str = rel.to_string_lossy().replace('\\', "/");
                    if key_str.starts_with(prefix) {
                        keys.push(ObjectKey::new(key_str));
                    }
                }
            }
        }
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(keys)
    }

    fn uri(&self) -> String {
        format!("file://{}", self.root.display())
    }
}

// ── S3-compatible backend ────────────────────────────────────────────────────

#[cfg(feature = "pitr_object_store")]
pub use s3_backend::S3Backend;

#[cfg(feature = "pitr_object_store")]
mod s3_backend {
    use super::{ObjectKey, ObjectStoreBackend};
    use object_store::{aws::AmazonS3Builder, path::Path as OsPath, ObjectStore};
    use std::sync::Arc;

    pub struct S3Backend {
        store: Arc<dyn ObjectStore>,
        bucket: String,
        prefix: String,
    }

    impl S3Backend {
        /// Create an S3-compatible backend.
        ///
        /// - `endpoint`: optional custom endpoint URL (for MinIO: `http://localhost:9000`)
        /// - `bucket`: S3 bucket name
        /// - `prefix`: key prefix for all objects (e.g. `"falcondb/wal/"`)
        /// - `access_key`, `secret_key`: credentials (or use env vars AWS_ACCESS_KEY_ID etc.)
        /// - `region`: AWS region or `"auto"` for MinIO
        pub fn new(
            endpoint: Option<&str>,
            bucket: &str,
            prefix: &str,
            access_key: Option<&str>,
            secret_key: Option<&str>,
            region: &str,
        ) -> Result<Self, String> {
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region);
            if let Some(ep) = endpoint {
                builder = builder.with_endpoint(ep).with_allow_http(true);
            }
            if let (Some(ak), Some(sk)) = (access_key, secret_key) {
                builder = builder.with_access_key_id(ak).with_secret_access_key(sk);
            }
            let store = builder.build().map_err(|e| e.to_string())?;
            Ok(Self {
                store: Arc::new(store),
                bucket: bucket.to_owned(),
                prefix: prefix.trim_end_matches('/').to_owned(),
            })
        }

        fn os_path(&self, key: &ObjectKey) -> OsPath {
            let full = if self.prefix.is_empty() {
                key.0.clone()
            } else {
                format!("{}/{}", self.prefix, key.0)
            };
            OsPath::from(full)
        }
    }

    impl std::fmt::Debug for S3Backend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "S3Backend(bucket={}, prefix={})", self.bucket, self.prefix)
        }
    }

    impl ObjectStoreBackend for S3Backend {
        fn put(&self, key: &ObjectKey, data: &[u8]) -> Result<(), String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            let payload = object_store::PutPayload::from(bytes::Bytes::copy_from_slice(data));
            rt.block_on(async move { store.put(&path, payload).await })
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                let result = store.get(&path).await.map_err(|e| e.to_string())?;
                result.bytes().await.map(|b| b.to_vec()).map_err(|e| e.to_string())
            })
        }

        fn exists(&self, key: &ObjectKey) -> Result<bool, String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                match store.head(&path).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn delete(&self, key: &ObjectKey) -> Result<(), String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                match store.delete(&path).await {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectKey>, String> {
            use futures::StreamExt;
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let full_prefix = if self.prefix.is_empty() {
                prefix.to_owned()
            } else {
                format!("{}/{}", self.prefix, prefix)
            };
            let list_path = OsPath::from(full_prefix.clone());
            rt.block_on(async move {
                let mut stream = store.list(Some(&list_path));
                let mut keys = vec![];
                while let Some(item) = stream.next().await {
                    let meta = item.map_err(|e| e.to_string())?;
                    let loc = meta.location.to_string();
                    let relative = if !self.prefix.is_empty() {
                        loc.strip_prefix(&format!("{}/", self.prefix))
                            .unwrap_or(&loc)
                            .to_owned()
                    } else {
                        loc
                    };
                    keys.push(ObjectKey::new(relative));
                }
                keys.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(keys)
            })
        }

        fn uri(&self) -> String {
            format!("s3://{}/{}", self.bucket, self.prefix)
        }
    }
}

// ── Azure Blob Storage backend ───────────────────────────────────────────────

#[cfg(feature = "pitr_object_store")]
pub use azure_backend::AzureBackend;

#[cfg(feature = "pitr_object_store")]
mod azure_backend {
    use super::{ObjectKey, ObjectStoreBackend};
    use object_store::{azure::MicrosoftAzureBuilder, path::Path as OsPath, ObjectStore};
    use std::sync::Arc;

    pub struct AzureBackend {
        store: Arc<dyn ObjectStore>,
        container: String,
        prefix: String,
    }

    impl AzureBackend {
        /// Create an Azure Blob Storage backend.
        pub fn new(
            account: &str,
            access_key: &str,
            container: &str,
            prefix: &str,
        ) -> Result<Self, String> {
            let store = MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_access_key(access_key)
                .with_container_name(container)
                .build()
                .map_err(|e| e.to_string())?;
            Ok(Self {
                store: Arc::new(store),
                container: container.to_owned(),
                prefix: prefix.trim_end_matches('/').to_owned(),
            })
        }

        fn os_path(&self, key: &ObjectKey) -> OsPath {
            let full = if self.prefix.is_empty() {
                key.0.clone()
            } else {
                format!("{}/{}", self.prefix, key.0)
            };
            OsPath::from(full)
        }
    }

    impl std::fmt::Debug for AzureBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AzureBackend(container={}, prefix={})", self.container, self.prefix)
        }
    }

    impl ObjectStoreBackend for AzureBackend {
        fn put(&self, key: &ObjectKey, data: &[u8]) -> Result<(), String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            let payload = object_store::PutPayload::from(bytes::Bytes::copy_from_slice(data));
            rt.block_on(async move { store.put(&path, payload).await })
                .map(|_| ())
                .map_err(|e| e.to_string())
        }

        fn get(&self, key: &ObjectKey) -> Result<Vec<u8>, String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                let result = store.get(&path).await.map_err(|e| e.to_string())?;
                result.bytes().await.map(|b| b.to_vec()).map_err(|e| e.to_string())
            })
        }

        fn exists(&self, key: &ObjectKey) -> Result<bool, String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                match store.head(&path).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn delete(&self, key: &ObjectKey) -> Result<(), String> {
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let path = self.os_path(key);
            rt.block_on(async move {
                match store.delete(&path).await {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectKey>, String> {
            use futures::StreamExt;
            let rt = tokio::runtime::Handle::try_current()
                .map_err(|_| "no tokio runtime".to_owned())?;
            let store = self.store.clone();
            let full_prefix = if self.prefix.is_empty() {
                prefix.to_owned()
            } else {
                format!("{}/{}", self.prefix, prefix)
            };
            let list_path = OsPath::from(full_prefix);
            rt.block_on(async move {
                use futures::StreamExt;
                let mut stream = store.list(Some(&list_path));
                let mut keys = vec![];
                while let Some(item) = stream.next().await {
                    let meta = item.map_err(|e| e.to_string())?;
                    let loc = meta.location.to_string();
                    let relative = if !self.prefix.is_empty() {
                        loc.strip_prefix(&format!("{}/", self.prefix))
                            .unwrap_or(&loc)
                            .to_owned()
                    } else {
                        loc
                    };
                    keys.push(ObjectKey::new(relative));
                }
                keys.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(keys)
            })
        }

        fn uri(&self) -> String {
            format!("az://{}/{}", self.container, self.prefix)
        }
    }
}

// ── Backend config (deserializable from falcon.toml) ────────────────────────

/// Selects which object store backend to use.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ObjectStoreConfig {
    /// Store archived WAL in the local filesystem under `path`.
    #[default]
    Local,
    /// S3-compatible storage (AWS S3, MinIO, Ceph).
    S3 {
        endpoint: Option<String>,
        bucket: String,
        #[serde(default)]
        prefix: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        #[serde(default = "default_region")]
        region: String,
    },
    /// Azure Blob Storage.
    Azure {
        account: String,
        access_key: String,
        container: String,
        #[serde(default)]
        prefix: String,
    },
}

fn default_region() -> String {
    "us-east-1".to_owned()
}

impl ObjectStoreConfig {
    /// Build a backend from this config.
    /// `local_path` is used when `type = local`.
    pub fn build(&self, local_path: &Path) -> Result<Box<dyn ObjectStoreBackend>, String> {
        match self {
            Self::Local => Ok(Box::new(LocalFsBackend::new(local_path))),
            #[cfg(feature = "pitr_object_store")]
            Self::S3 { endpoint, bucket, prefix, access_key, secret_key, region } => {
                Ok(Box::new(S3Backend::new(
                    endpoint.as_deref(),
                    bucket,
                    prefix,
                    access_key.as_deref(),
                    secret_key.as_deref(),
                    region,
                )?))
            }
            #[cfg(feature = "pitr_object_store")]
            Self::Azure { account, access_key, container, prefix } => {
                Ok(Box::new(AzureBackend::new(account, access_key, container, prefix)?))
            }
            #[cfg(not(feature = "pitr_object_store"))]
            Self::S3 { .. } | Self::Azure { .. } => Err(
                "S3/Azure backends require --features pitr_object_store".to_owned()
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("falcondb_osb_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_local_put_get_exists_delete() {
        let root = tmp_dir("local_basic");
        let backend = LocalFsBackend::new(&root);
        let key = ObjectKey::new("wal/seg001.wal");
        let data = b"hello wal segment";

        assert!(!backend.exists(&key).unwrap());
        backend.put(&key, data).unwrap();
        assert!(backend.exists(&key).unwrap());
        assert_eq!(backend.get(&key).unwrap(), data);
        backend.delete(&key).unwrap();
        assert!(!backend.exists(&key).unwrap());
    }

    #[test]
    fn test_local_list_prefix() {
        let root = tmp_dir("local_list");
        let backend = LocalFsBackend::new(&root);

        backend.put(&ObjectKey::new("wal/001.wal"), b"a").unwrap();
        backend.put(&ObjectKey::new("wal/002.wal"), b"b").unwrap();
        backend.put(&ObjectKey::new("other/x.dat"), b"c").unwrap();

        let keys = backend.list_prefix("wal/").unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().any(|k| k.0 == "wal/001.wal"));
        assert!(keys.iter().any(|k| k.0 == "wal/002.wal"));
    }

    #[test]
    fn test_local_uri() {
        let backend = LocalFsBackend::new("/tmp/archive");
        assert!(backend.uri().starts_with("file://"));
    }

    #[test]
    fn test_config_build_local() {
        let cfg = ObjectStoreConfig::Local;
        let backend = cfg.build(Path::new("/tmp/pitr")).unwrap();
        assert!(backend.uri().starts_with("file://"));
    }

    #[test]
    fn test_config_s3_without_feature_returns_err() {
        let cfg = ObjectStoreConfig::S3 {
            endpoint: None,
            bucket: "mybucket".to_owned(),
            prefix: "wal".to_owned(),
            access_key: None,
            secret_key: None,
            region: "us-east-1".to_owned(),
        };
        #[cfg(not(feature = "pitr_object_store"))]
        assert!(cfg.build(Path::new("/tmp")).is_err());
        #[cfg(feature = "pitr_object_store")]
        let _ = cfg; // skip: would need real credentials
    }
}
