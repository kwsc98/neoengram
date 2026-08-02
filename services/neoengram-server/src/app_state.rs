use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

use neoengram_protocol::{PrincipalId, PrincipalKind, PrincipalRef};

/// Shared application state available to all Fusen interface handlers.
pub struct AppState {
    pub authority: Arc<neoengramd::SqliteAuthority>,
    pub control: std::sync::Arc<neoengramd::ControlPlane>,
    pub ready: AtomicBool,
    pub dev_principal: PrincipalRef,
}

impl AppState {
    pub fn new(
        authority: neoengramd::SqliteAuthority,
        control: neoengramd::ControlPlane,
        dev_principal_id: &str,
    ) -> Self {
        let principal_id = PrincipalId::new(dev_principal_id)
            .unwrap_or_else(|_| PrincipalId::new("neoengram-server-dev").unwrap());
        let dev_principal = PrincipalRef {
            kind: PrincipalKind::Service,
            id: principal_id,
            extensions: Default::default(),
        };
        Self {
            authority: Arc::new(authority),
            control: Arc::new(control),
            ready: AtomicBool::new(false),
            dev_principal,
        }
    }

    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub fn set_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// Opens the SQLite authority from a data directory.
pub async fn open_authority(data_dir: &str) -> Result<neoengramd::SqliteAuthority, String> {
    let db_path = std::path::PathBuf::from(data_dir).join("authority.sqlite3");
    std::fs::create_dir_all(data_dir)
        .map_err(|e| format!("failed to create data directory {data_dir}: {e}"))?;

    let config = neoengramd::SqliteAuthorityConfig::new(db_path);
    neoengramd::open_sqlite_authority(config)
        .await
        .map_err(|e| format!("failed to open authority database: {e}"))
}

/// Builds a `ControlPlane` composed from the authority and a system clock.
pub fn build_control_plane(authority: &neoengramd::SqliteAuthority) -> neoengramd::ControlPlane {
    let clock = Arc::new(crate::clock::SystemClock);
    neoengramd::ControlPlane::new(
        Arc::new(neoengramd::AllowAllAuthorizer),
        authority.authority_store(),
        clock,
    )
}
