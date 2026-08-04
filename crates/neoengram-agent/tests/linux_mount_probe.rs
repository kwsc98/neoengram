#![cfg(target_os = "linux")]

use std::{env, fs, path::PathBuf};

use neoengram_agent::{FilesystemMountProbeConfig, MountProbeCondition, VOLUME_MARKER_FILE_NAME};
use neoengram_protocol::{MountAccessMode, ResourceHealth, VolumeMarkerId};

const REAL_MOUNT_ROOT_ENV: &str = "NEOENGRAM_REAL_MOUNT_PROBE_ROOT";
const EXPECTED_MARKER: &str = "neoengram-ci-volume";

#[test]
#[ignore = "requires a real Linux mount; the Ubuntu CI job provisions a tmpfs"]
fn detects_real_mount_boundary_and_read_write_capabilities() {
    let mount_root = PathBuf::from(
        env::var_os(REAL_MOUNT_ROOT_ENV)
            .unwrap_or_else(|| panic!("{REAL_MOUNT_ROOT_ENV} must name the prepared mount root")),
    );
    assert!(mount_root.is_absolute());
    assert_eq!(
        fs::read_to_string(mount_root.join(VOLUME_MARKER_FILE_NAME)).unwrap(),
        format!("{EXPECTED_MARKER}\n")
    );

    let config = FilesystemMountProbeConfig {
        mount_root: mount_root.clone(),
        expected_volume_marker: VolumeMarkerId::new(EXPECTED_MARKER).unwrap(),
        desired_access_mode: MountAccessMode::ReadWrite,
        hard_minimum_free_bytes: 1,
        ready_minimum_free_bytes: 1,
    };

    let first = config.probe();
    assert_eq!(first.condition, MountProbeCondition::Ready);
    assert_eq!(first.health, ResourceHealth::Ready);
    assert_eq!(
        first.observed_volume_marker,
        Some(VolumeMarkerId::new(EXPECTED_MARKER).unwrap())
    );
    assert!(first.marker_matches);
    assert!(first.mount_boundary_detected);
    assert_eq!(first.access_mode, Some(MountAccessMode::ReadWrite));
    assert!(first.rename_supported);
    assert!(first.fsync_supported);
    assert!(first.available_bytes.is_some_and(|bytes| bytes > 0));
    assert!(first.mount_identity_digest.is_some());

    let second = config.probe();
    assert_eq!(second.condition, MountProbeCondition::Ready);
    assert_eq!(second.mount_identity_digest, first.mount_identity_digest);

    let remaining = fs::read_dir(mount_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(
        remaining,
        vec![std::ffi::OsString::from(VOLUME_MARKER_FILE_NAME)]
    );
}
