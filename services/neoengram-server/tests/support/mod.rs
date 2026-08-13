use async_trait::async_trait;
use neoengram_protocol::{StorageVolumeId, TenantId};
use neoengram_server::StorageAvailabilityProvider;
use neoengramd::{CentralResult, DerivedVolumeState};

pub struct ReadyStorageAvailability;

#[async_trait]
impl StorageAvailabilityProvider for ReadyStorageAvailability {
    async fn current_volume_state(
        &self,
        _tenant_id: &TenantId,
        _storage_volume_id: &StorageVolumeId,
    ) -> CentralResult<DerivedVolumeState> {
        Ok(DerivedVolumeState::Ready)
    }
}
