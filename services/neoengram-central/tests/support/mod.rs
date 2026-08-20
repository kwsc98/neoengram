use async_trait::async_trait;
use neoengram_central::StorageAvailabilityProvider;
use neoengram_central::{CentralResult, DerivedVolumeState};
use neoengram_domain::protocol::{StorageVolumeId, TenantId};

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
