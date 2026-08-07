use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use neoengram_protocol::{
    AgentEnrollmentTokenId, ContentDigest, EdgeClusterId, PvcIdentityDigest, StorageVolumeId,
    TenantId, VolumeMarkerId,
};
use serde::Deserialize;
use url::Url;

use crate::{AgentDaemonError, AgentDaemonResult};

const CONFIG_SCHEMA_VERSION: u16 = 1;
const PROTOCOL_VERSION: u16 = 1;
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
const MAX_REGION_BYTES: usize = 128;
const MAX_LOG_LEVEL_BYTES: usize = 128;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub schema_version: u16,
    pub protocol_version: u16,
    pub central_endpoint: Url,
    pub tenant_id: TenantId,
    pub edge_cluster_id: EdgeClusterId,
    pub storage_volume_id: StorageVolumeId,
    pub volume_descriptor_digest: ContentDigest,
    pub region: String,
    pub storage: StorageConfig,
    pub registration: RegistrationConfig,
    pub session: SessionConfig,
    pub logging: LoggingConfig,
}

impl AgentConfig {
    pub fn load(path: impl AsRef<Path>) -> AgentDaemonResult<Self> {
        let path = path.as_ref();
        // Kubernetes projects ConfigMap entries through symlinks, so inspect the resolved target.
        let metadata = fs::metadata(path).map_err(|source| AgentDaemonError::ConfigIo {
            path: path.to_path_buf(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(AgentDaemonError::Configuration(
                "configuration path must resolve to a regular file".to_owned(),
            ));
        }
        if metadata.len() > MAX_CONFIG_BYTES {
            return Err(AgentDaemonError::Configuration(format!(
                "configuration exceeds the {MAX_CONFIG_BYTES}-byte limit"
            )));
        }
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        fs::File::open(path)
            .and_then(|file| file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes))
            .map_err(|source| AgentDaemonError::ConfigIo {
                path: path.to_path_buf(),
                source,
            })?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(AgentDaemonError::Configuration(format!(
                "configuration exceeds the {MAX_CONFIG_BYTES}-byte limit"
            )));
        }
        let config: Self = serde_yaml::from_slice(&bytes)
            .map_err(|error| AgentDaemonError::ConfigDecode(error.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> AgentDaemonResult<()> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(configuration("schema_version must be 1"));
        }
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(configuration("protocol_version must be 1"));
        }
        validate_endpoint(&self.central_endpoint)?;
        validate_region(&self.region)?;
        self.storage.validate(&self.storage_volume_id)?;
        self.registration.validate()?;
        self.session.validate()?;
        self.logging.validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub backend_type: StorageBackendType,
    pub access_mode: StorageAccessMode,
    pub mount_path: PathBuf,
    pub state_dir: PathBuf,
    pub marker_file: PathBuf,
    pub expected_volume_marker: VolumeMarkerId,
    pub pvc_reference: PvcReference,
    #[serde(default)]
    pub hard_minimum_free_bytes: u64,
    #[serde(default)]
    pub ready_minimum_free_bytes: u64,
}

impl StorageConfig {
    fn validate(&self, storage_volume_id: &StorageVolumeId) -> AgentDaemonResult<()> {
        if self.backend_type != StorageBackendType::Pvc {
            return Err(configuration("only the pvc storage backend is supported"));
        }
        validate_absolute_normal_path("storage.mount_path", &self.mount_path)?;
        validate_absolute_normal_path("storage.state_dir", &self.state_dir)?;
        validate_absolute_normal_path("storage.marker_file", &self.marker_file)?;
        if self.mount_path == self.state_dir
            || self.state_dir.starts_with(&self.mount_path)
            || self.mount_path.starts_with(&self.state_dir)
        {
            return Err(configuration(
                "storage.state_dir and storage.mount_path must be separate mount trees",
            ));
        }
        let expected_marker_file = self
            .mount_path
            .join(neoengram_agent::VOLUME_MARKER_FILE_NAME);
        if self.marker_file != expected_marker_file {
            return Err(configuration(
                "storage.marker_file must name .neoengram-volume-marker at the mount root",
            ));
        }
        if self.expected_volume_marker.as_str() != storage_volume_id.as_str() {
            return Err(configuration(
                "storage.expected_volume_marker must equal storage_volume_id",
            ));
        }
        if self.ready_minimum_free_bytes < self.hard_minimum_free_bytes {
            return Err(configuration(
                "storage.ready_minimum_free_bytes cannot be below hard_minimum_free_bytes",
            ));
        }
        self.pvc_reference.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackendType {
    Pvc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageAccessMode {
    ReadWriteOnce,
    ReadWriteMany,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PvcReference {
    pub namespace: String,
    pub claim_name: String,
}

impl PvcReference {
    fn validate(&self) -> AgentDaemonResult<()> {
        PvcIdentityDigest::derive(&self.namespace, &self.claim_name)
            .map(|_| ())
            .map_err(|error| configuration(format!("storage.pvc_reference is invalid: {error}")))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistrationConfig {
    pub approval_required: bool,
    pub token_id: AgentEnrollmentTokenId,
    pub bootstrap_token_file: PathBuf,
}

impl RegistrationConfig {
    fn validate(&self) -> AgentDaemonResult<()> {
        if !self.approval_required {
            return Err(configuration(
                "registration.approval_required must be true in enrollment protocol v1",
            ));
        }
        validate_absolute_normal_path(
            "registration.bootstrap_token_file",
            &self.bootstrap_token_file,
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    pub heartbeat_interval_seconds: u64,
    pub reconnect_max_delay_seconds: u64,
}

impl SessionConfig {
    fn validate(&self) -> AgentDaemonResult<()> {
        if self.heartbeat_interval_seconds == 0 || self.heartbeat_interval_seconds > 300 {
            return Err(configuration(
                "session.heartbeat_interval_seconds must be in 1..=300",
            ));
        }
        if self.reconnect_max_delay_seconds == 0 || self.reconnect_max_delay_seconds > 30 {
            return Err(configuration(
                "session.reconnect_max_delay_seconds must be in 1..=30",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    pub format: LoggingFormat,
    pub level: String,
}

impl LoggingConfig {
    fn validate(&self) -> AgentDaemonResult<()> {
        if self.level.is_empty()
            || self.level.len() > MAX_LOG_LEVEL_BYTES
            || self.level.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(configuration(
                "logging.level must be a non-empty, bounded tracing filter",
            ));
        }
        tracing_subscriber::EnvFilter::try_new(&self.level)
            .map_err(|error| configuration(format!("logging.level is invalid: {error}")))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoggingFormat {
    Json,
    Pretty,
}

fn validate_endpoint(endpoint: &Url) -> AgentDaemonResult<()> {
    if endpoint.cannot_be_a_base()
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.path() != "/"
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(configuration(
            "central_endpoint must be an origin URL without credentials, path, query, or fragment",
        ));
    }
    let loopback_http = endpoint.scheme() == "http" && has_loopback_host(endpoint);
    if endpoint.scheme() != "https" && !loopback_http {
        return Err(configuration(
            "central_endpoint must use HTTPS (HTTP is allowed only on loopback)",
        ));
    }
    Ok(())
}

pub(crate) fn validate_development_directory_probe_endpoint(
    endpoint: &Url,
) -> AgentDaemonResult<()> {
    validate_endpoint(endpoint)?;
    if !has_loopback_host(endpoint) {
        return Err(configuration(
            "development directory probe requires a loopback central_endpoint",
        ));
    }
    Ok(())
}

fn has_loopback_host(endpoint: &Url) -> bool {
    match endpoint.host() {
        Some(url::Host::Ipv4(address)) => address == std::net::Ipv4Addr::LOCALHOST,
        Some(url::Host::Ipv6(address)) => address == std::net::Ipv6Addr::LOCALHOST,
        Some(url::Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

fn validate_region(region: &str) -> AgentDaemonResult<()> {
    if region.is_empty()
        || region.len() > MAX_REGION_BYTES
        || region
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
    {
        return Err(configuration(
            "region must contain 1..=128 ASCII letters, digits, '.', '_', or '-'",
        ));
    }
    Ok(())
}

fn validate_absolute_normal_path(name: &str, path: &Path) -> AgentDaemonResult<()> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::CurDir | Component::Prefix(_)
            )
        })
    {
        return Err(configuration(format!(
            "{name} must be an absolute normalized path"
        )));
    }
    Ok(())
}

fn configuration(message: impl Into<String>) -> AgentDaemonError {
    AgentDaemonError::Configuration(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
schema_version: 1
protocol_version: 1
central_endpoint: https://neoengram.example.internal
tenant_id: tenant-example
edge_cluster_id: edge-example
storage_volume_id: volume-example
volume_descriptor_digest: 0000000000000000000000000000000000000000000000000000000000000000
region: cn-example-1
storage:
  backend_type: pvc
  access_mode: read_write_many
  mount_path: /volume
  state_dir: /var/lib/neoengram-agent
  marker_file: /volume/.neoengram-volume-marker
  expected_volume_marker: volume-example
  pvc_reference:
    namespace: neoengram-agent-example
    claim_name: business-pvc
registration:
  approval_required: true
  token_id: token-example
  bootstrap_token_file: /var/run/secrets/neoengram/bootstrap-token
session:
  heartbeat_interval_seconds: 10
  reconnect_max_delay_seconds: 30
logging:
  format: json
  level: info
"#;

    #[test]
    fn loads_the_kubernetes_configuration_shape() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("agent.yaml");
        fs::write(&path, VALID_CONFIG).unwrap();
        let config = AgentConfig::load(path).unwrap();
        assert_eq!(config.storage.access_mode, StorageAccessMode::ReadWriteMany);
        assert_eq!(config.registration.token_id.as_str(), "token-example");
    }

    #[test]
    fn rejects_unknown_and_duplicate_fields() {
        let unknown = VALID_CONFIG.replace("schema_version: 1", "schema_version: 1\nextra: true");
        assert!(serde_yaml::from_str::<AgentConfig>(&unknown).is_err());
        let duplicate =
            VALID_CONFIG.replace("schema_version: 1", "schema_version: 1\nschema_version: 1");
        assert!(serde_yaml::from_str::<AgentConfig>(&duplicate).is_err());
    }

    #[test]
    fn rejects_unsafe_paths_and_insecure_remote_http() {
        let mut config: AgentConfig = serde_yaml::from_str(VALID_CONFIG).unwrap();
        config.storage.marker_file = PathBuf::from("/volume/../marker");
        assert!(config.validate().is_err());

        let mut config: AgentConfig = serde_yaml::from_str(VALID_CONFIG).unwrap();
        config.central_endpoint = Url::parse("http://central.example/agent").unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn development_directory_probe_accepts_only_loopback_endpoints() {
        for endpoint in [
            "http://127.0.0.1:8081/",
            "http://[::1]:8081/",
            "https://localhost:8081/",
        ] {
            validate_development_directory_probe_endpoint(&Url::parse(endpoint).unwrap()).unwrap();
        }

        for endpoint in [
            "https://neoengram.example.internal/",
            "https://localhost.example:8081/",
            "http://127.0.0.1:8081/not-an-origin",
        ] {
            assert!(
                validate_development_directory_probe_endpoint(&Url::parse(endpoint).unwrap())
                    .is_err(),
                "unexpectedly accepted {endpoint}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn accepts_a_kubernetes_style_symlinked_configuration_file() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("real.yaml");
        let link = directory.path().join("link.yaml");
        fs::write(&real, VALID_CONFIG).unwrap();
        symlink(real, &link).unwrap();
        AgentConfig::load(link).unwrap();
    }
}
