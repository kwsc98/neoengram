use std::sync::Arc;

use neoengram_protocol::{CertificateGeneration, GatewayPoolId, ResourceVersion, UnixMillis};
use neoengramd::{
    CentralError, CentralErrorCode, CentralResult, Clock, GatewayCredentialState,
    GatewayPoolListRequest, GatewayRegistryRepository, GatewayReplicaListRequest,
    GatewayReplicaRecord, GatewayReplicaState, GATEWAY_REGISTRY_MAX_PAGE_SIZE,
};

/// Result of one bounded scan of the authoritative Gateway credential registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewayCredentialExpiryRun {
    pub examined: usize,
    pub revoked: usize,
    /// Records that changed between discovery and the revocation CAS. They are retried next pass.
    pub contended: usize,
}

/// Fences Gateway replicas whose active workload leaf has reached its wall-clock expiry.
///
/// Renewal is deliberately not performed here. Promoting a new certificate generation before the
/// exact chain is installed by the Replica would immediately strand the workload. Issuance and
/// delivery remain behind the external workload issuer/provisioner boundary; this service owns the
/// independent fail-closed expiry path that is always safe to run.
#[derive(Clone)]
pub struct GatewayCredentialLifecycleService {
    repository: Arc<dyn GatewayRegistryRepository>,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for GatewayCredentialLifecycleService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GatewayCredentialLifecycleService")
            .finish_non_exhaustive()
    }
}

impl GatewayCredentialLifecycleService {
    #[must_use]
    pub fn new(repository: Arc<dyn GatewayRegistryRepository>, clock: Arc<dyn Clock>) -> Self {
        Self { repository, clock }
    }

    /// Scans every Active/Draining Replica and revokes expired credentials with a Registry CAS.
    ///
    /// Revocation advances the certificate generation before changing the Replica state. Existing
    /// Central sessions re-read both facts for every frame, while the connector supervisor removes
    /// the old generation from its desired connection set.
    pub async fn reconcile_expired_credentials(&self) -> CentralResult<GatewayCredentialExpiryRun> {
        let now = self.clock.now();
        let mut run = GatewayCredentialExpiryRun::default();
        let mut pool_after = None;

        loop {
            let pools = self
                .repository
                .list_pools(&GatewayPoolListRequest {
                    edge_cluster_id: None,
                    state: None,
                    after: pool_after.clone(),
                    limit: GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                })
                .await?;
            if pools.is_empty() {
                break;
            }
            for pool in &pools {
                for state in [GatewayReplicaState::Active, GatewayReplicaState::Draining] {
                    self.reconcile_pool_state(&pool.gateway_pool_id, state, now, &mut run)
                        .await?;
                }
            }
            if pools.len() < GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                break;
            }
            pool_after = pools.last().map(|pool| pool.gateway_pool_id.clone());
        }
        Ok(run)
    }

    async fn reconcile_pool_state(
        &self,
        gateway_pool_id: &GatewayPoolId,
        state: GatewayReplicaState,
        now: UnixMillis,
        run: &mut GatewayCredentialExpiryRun,
    ) -> CentralResult<()> {
        let mut replica_after = None;
        loop {
            let replicas = self
                .repository
                .list_replicas(&GatewayReplicaListRequest {
                    gateway_pool_id: gateway_pool_id.clone(),
                    state: Some(state),
                    after: replica_after.clone(),
                    limit: GATEWAY_REGISTRY_MAX_PAGE_SIZE,
                })
                .await?;
            if replicas.is_empty() {
                break;
            }
            for replica in &replicas {
                run.examined += 1;
                if replica.credential.state != GatewayCredentialState::Active
                    || replica
                        .credential
                        .certificate_not_after_unix_ms
                        .is_none_or(|not_after| not_after.get() > now.get())
                {
                    continue;
                }
                let next = expired_replica_fence(replica, now)?;
                match self
                    .repository
                    .replace_replica(replica.resource_version.get(), next)
                    .await
                {
                    Ok(_) => run.revoked += 1,
                    Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                        run.contended += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
            if replicas.len() < GATEWAY_REGISTRY_MAX_PAGE_SIZE {
                break;
            }
            replica_after = replicas
                .last()
                .map(|replica| replica.gateway_replica_id.clone());
        }
        Ok(())
    }
}

fn expired_replica_fence(
    replica: &GatewayReplicaRecord,
    now: UnixMillis,
) -> CentralResult<GatewayReplicaRecord> {
    let mut next = replica.clone();
    let generation = next.credential.certificate_generation.ok_or_else(|| {
        lifecycle_error("active Gateway credential has no certificate generation")
    })?;
    let next_generation = generation
        .get()
        .checked_add(1)
        .ok_or_else(|| lifecycle_error("Gateway certificate generation exhausted"))?;
    let next_resource_version = next
        .resource_version
        .get()
        .checked_add(1)
        .ok_or_else(|| lifecycle_error("GatewayReplica ResourceVersion exhausted"))?;
    next.state = GatewayReplicaState::Revoked;
    next.credential.state = GatewayCredentialState::Revoked;
    next.credential.certificate_generation = Some(CertificateGeneration::new(next_generation));
    next.resource_version = ResourceVersion::new(next_resource_version);
    next.updated_at_unix_ms = now;
    Ok(next)
}

fn lifecycle_error(message: &'static str) -> CentralError {
    CentralError::new(CentralErrorCode::Internal, message).with_retryable(false)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use neoengram_core::ContentDigest;
    use neoengram_protocol::{
        EdgeClusterId, Extensions, GatewayOpaqueBytes, GatewayPoolId, GatewayReplicaId, Generation,
        PrincipalId, PrincipalKind, PrincipalRef, ProtocolVersion, RequestId,
    };
    use neoengramd::{
        GatewayInsertOutcome, GatewayPoolRecord, GatewayPoolState, GatewayReplicaCertificateRecord,
        GatewayReplicaCredential, InMemoryClock, InMemoryGatewayRegistry,
    };

    use super::*;

    fn actor() -> PrincipalRef {
        PrincipalRef {
            kind: PrincipalKind::System,
            id: PrincipalId::new("gateway-lifecycle-test").unwrap(),
            extensions: Extensions::new(),
        }
    }

    fn pool() -> GatewayPoolRecord {
        GatewayPoolRecord {
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            display_name: "Pool A".into(),
            agent_endpoint: "https://gateway.example.test".into(),
            s3_endpoint: None,
            desired_replicas: 2,
            minimum_ready_replicas: 1,
            state: GatewayPoolState::Ready,
            config_generation: Generation::new(1),
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
            created_by: actor(),
            updated_by: actor(),
        }
    }

    fn pending_replica(id: &str) -> GatewayReplicaRecord {
        GatewayReplicaRecord {
            gateway_replica_id: GatewayReplicaId::new(id).unwrap(),
            gateway_pool_id: GatewayPoolId::new("pool-a").unwrap(),
            edge_cluster_id: EdgeClusterId::new("edge-a").unwrap(),
            control_endpoint: format!("https://{id}.control.example.test"),
            peer_endpoint: format!("https://{id}.peer.example.test"),
            bootstrap_endpoint: format!("https://{id}.bootstrap.example.test"),
            software_version: "test".into(),
            supported_protocol_versions: BTreeSet::from([ProtocolVersion::V1]),
            capabilities: BTreeSet::from(["agent_edge".into()]),
            last_heartbeat_at_unix_ms: None,
            state: GatewayReplicaState::Pending,
            credential: GatewayReplicaCredential {
                activation_token_digest: ContentDigest::hash(format!("token-{id}").as_bytes()),
                activation_created_at_unix_ms: UnixMillis::new(1_000),
                activation_expires_at_unix_ms: UnixMillis::new(901_000),
                activation_consumed_at_unix_ms: None,
                public_key_fingerprint: None,
                certificate_generation: None,
                certificate_fingerprint: None,
                certificate_not_after_unix_ms: None,
                certificate: None,
                state: GatewayCredentialState::PendingActivation,
            },
            resource_version: ResourceVersion::new(1),
            created_at_unix_ms: UnixMillis::new(1_000),
            updated_at_unix_ms: UnixMillis::new(1_000),
        }
    }

    async fn install_replica(
        repository: &InMemoryGatewayRegistry,
        id: &str,
        state: GatewayReplicaState,
        not_after: u64,
    ) {
        let pending = pending_replica(id);
        repository.insert_replica(pending.clone()).await.unwrap();
        let leaf = GatewayOpaqueBytes::new(format!("leaf-{id}").into_bytes()).unwrap();
        let public_key = neoengram_protocol::Ed25519PublicKeySpki::from_public_key_bytes([7; 32]);
        let certificate = GatewayReplicaCertificateRecord {
            request_id: RequestId::new(format!("certificate-{id}")).unwrap(),
            public_key_spki: public_key.clone(),
            certificate_generation: CertificateGeneration::new(1),
            not_before_unix_ms: UnixMillis::new(2_000),
            not_after_unix_ms: UnixMillis::new(not_after),
            server_names: BTreeSet::new(),
            leaf_certificate_der: leaf.clone(),
            issuer_chain_der: vec![GatewayOpaqueBytes::new(b"issuer".to_vec()).unwrap()],
        };
        let mut prepared = pending;
        prepared.credential.public_key_fingerprint = Some(public_key.fingerprint());
        prepared.credential.certificate_generation = Some(CertificateGeneration::new(1));
        prepared.credential.certificate_fingerprint = Some(ContentDigest::hash(leaf.as_bytes()));
        prepared.credential.certificate_not_after_unix_ms = Some(UnixMillis::new(not_after));
        prepared.credential.certificate = Some(certificate);
        prepared.credential.state = GatewayCredentialState::PendingCertificateDelivery;
        prepared.resource_version = ResourceVersion::new(2);
        prepared.updated_at_unix_ms = UnixMillis::new(1_500);
        let prepared = repository.replace_replica(1, prepared).await.unwrap();

        let mut active = prepared;
        active.state = GatewayReplicaState::Active;
        active.credential.activation_consumed_at_unix_ms = Some(UnixMillis::new(2_000));
        active.credential.state = GatewayCredentialState::Active;
        active.resource_version = ResourceVersion::new(3);
        active.updated_at_unix_ms = UnixMillis::new(2_000);
        let mut active = repository.replace_replica(2, active).await.unwrap();
        if state == GatewayReplicaState::Draining {
            active.state = GatewayReplicaState::Draining;
            active.resource_version = ResourceVersion::new(4);
            active.updated_at_unix_ms = UnixMillis::new(2_001);
            repository.replace_replica(3, active).await.unwrap();
        }
    }

    #[tokio::test]
    async fn expired_active_and_draining_credentials_are_fenced_once() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        assert!(matches!(
            repository.insert_pool(pool()).await.unwrap(),
            GatewayInsertOutcome::Inserted(_)
        ));
        install_replica(
            &repository,
            "replica-active",
            GatewayReplicaState::Active,
            10_000,
        )
        .await;
        install_replica(
            &repository,
            "replica-draining",
            GatewayReplicaState::Draining,
            9_999,
        )
        .await;
        install_replica(
            &repository,
            "replica-valid",
            GatewayReplicaState::Active,
            10_001,
        )
        .await;
        let clock = Arc::new(InMemoryClock::new(10_000));
        let service = GatewayCredentialLifecycleService::new(repository.clone(), clock);

        let first = service.reconcile_expired_credentials().await.unwrap();
        assert_eq!(first.examined, 3);
        assert_eq!(first.revoked, 2);
        assert_eq!(first.contended, 0);
        for id in ["replica-active", "replica-draining"] {
            let stored = repository
                .get_replica(&GatewayReplicaId::new(id).unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.state, GatewayReplicaState::Revoked);
            assert_eq!(stored.credential.state, GatewayCredentialState::Revoked);
            assert_eq!(
                stored.credential.certificate_generation,
                Some(CertificateGeneration::new(2))
            );
            let expected_version = if id == "replica-draining" { 5 } else { 4 };
            assert_eq!(
                stored.resource_version,
                ResourceVersion::new(expected_version)
            );
        }
        let valid = repository
            .get_replica(&GatewayReplicaId::new("replica-valid").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid.state, GatewayReplicaState::Active);
        assert_eq!(
            valid.credential.certificate_generation,
            Some(CertificateGeneration::new(1))
        );

        let second = service.reconcile_expired_credentials().await.unwrap();
        assert_eq!(second.examined, 1);
        assert_eq!(second.revoked, 0);
    }

    #[tokio::test]
    async fn disabled_pool_expiry_reconciliation_fences_and_continues() {
        let repository = Arc::new(InMemoryGatewayRegistry::new());
        assert!(matches!(
            repository.insert_pool(pool()).await.unwrap(),
            GatewayInsertOutcome::Inserted(_)
        ));
        install_replica(
            &repository,
            "replica-expired",
            GatewayReplicaState::Active,
            10_000,
        )
        .await;
        install_replica(
            &repository,
            "replica-valid",
            GatewayReplicaState::Active,
            10_001,
        )
        .await;

        let mut disabled = repository
            .get_pool(&GatewayPoolId::new("pool-a").unwrap())
            .await
            .unwrap()
            .unwrap();
        disabled.state = GatewayPoolState::Disabled;
        disabled.config_generation = Generation::new(2);
        disabled.resource_version = ResourceVersion::new(2);
        disabled.updated_at_unix_ms = UnixMillis::new(3_000);
        disabled.updated_by = actor();
        repository.replace_pool(1, disabled).await.unwrap();

        let service = GatewayCredentialLifecycleService::new(
            repository.clone(),
            Arc::new(InMemoryClock::new(10_000)),
        );
        let run = service.reconcile_expired_credentials().await.unwrap();
        assert_eq!(run.examined, 2);
        assert_eq!(run.revoked, 1);
        assert_eq!(run.contended, 0);
        assert_eq!(
            repository
                .get_replica(&GatewayReplicaId::new("replica-expired").unwrap())
                .await
                .unwrap()
                .unwrap()
                .state,
            GatewayReplicaState::Revoked
        );
        assert_eq!(
            repository
                .get_replica(&GatewayReplicaId::new("replica-valid").unwrap())
                .await
                .unwrap()
                .unwrap()
                .state,
            GatewayReplicaState::Active
        );
    }
}
