use std::sync::Arc;

use crate::{AgentRegistryRecord, AgentRegistryService, CentralError, CentralErrorCode};
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{
    AgentBootstrapStatusRequest, AgentBootstrapStatusResponse, AgentBootstrapStatusState,
    AgentWorkloadCertificateBundle, AgentWorkloadCertificateDer, CertificateGeneration, Extensions,
    RequestId, SessionGeneration, UnixMillis,
};
use thiserror::Error;

use super::{
    validate_issued_workload_certificate, WorkloadCertificateIssuance, WorkloadCertificateIssuer,
    WorkloadCertificateIssuerError, WorkloadCertificateRequest, WorkloadIdentityUri,
    WorkloadPkiError,
};

/// Central adapter that issues and durably prepares an approved Agent's first mTLS credential.
///
/// The Registry remains authoritative for the approved identity and public key. The online
/// Intermediate is invoked only after those bindings are loaded, and the resulting public bundle
/// is committed with a Registry ResourceVersion CAS before it is returned to the Agent.
#[derive(Clone)]
pub struct AgentWorkloadCertificateService {
    registry: Arc<AgentRegistryService>,
    issuance: WorkloadCertificateIssuance,
    trust_domain: String,
}

impl std::fmt::Debug for AgentWorkloadCertificateService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentWorkloadCertificateService")
            .field("trust_domain", &self.trust_domain)
            .finish_non_exhaustive()
    }
}

impl AgentWorkloadCertificateService {
    pub fn new(
        registry: Arc<AgentRegistryService>,
        issuer: Arc<dyn WorkloadCertificateIssuer>,
        trust_domain: impl Into<String>,
    ) -> Result<Self, WorkloadPkiError> {
        let trust_domain = trust_domain.into();
        // Constructing a representative URI applies the shared canonical trust-domain checks.
        WorkloadIdentityUri::agent(
            &trust_domain,
            neoengram_domain::protocol::EdgeClusterId::new("validation-edge")
                .expect("static EdgeCluster ID is valid"),
            neoengram_domain::protocol::AgentId::new("validation-agent")
                .expect("static Agent ID is valid"),
        )?;
        Ok(Self {
            registry,
            issuance: WorkloadCertificateIssuance::new(issuer),
            trust_domain,
        })
    }

    /// Attaches a replayable certificate bundle to an approved status response.
    pub async fn attach_to_status(
        &self,
        request: &AgentBootstrapStatusRequest,
        mut response: AgentBootstrapStatusResponse,
    ) -> Result<AgentBootstrapStatusResponse, AgentWorkloadCertificateError> {
        if response.state != AgentBootstrapStatusState::Approved {
            return Ok(response);
        }
        let record = self
            .registry
            .query_enrollment(&request.tenant_id, &response.enrollment_id)
            .await?;
        let now = self.registry.now();
        let generation = generation_to_issue(
            record.workload_certificate.as_ref().map(|certificate| {
                (
                    certificate.certificate_generation,
                    certificate.renew_at_unix_ms,
                )
            }),
            now,
        )?;
        let Some(generation) = generation else {
            if let Some(certificate) = record.workload_certificate.as_ref() {
                response.resource_version = record.resource_version;
                response.certificate = Some(certificate.clone());
                response.validate()?;
                return Ok(response);
            }
            return Err(AgentWorkloadCertificateError::ConcurrentUpdate);
        };
        let mut certificate = self.issue_for_record(&record, generation).await?;
        let mut expected_resource_version = record.resource_version;
        // Unrelated Registry updates may race certificate delivery. Reuse the exact issued bundle
        // across CAS retries, and prefer the winner if another issuer request committed first.
        for _ in 0..3 {
            match self
                .registry
                .install_workload_certificate(expected_resource_version, certificate.clone())
                .await
            {
                Ok(installed) => {
                    response.resource_version = installed.resource_version;
                    response.certificate = installed.workload_certificate;
                    response.validate()?;
                    return Ok(response);
                }
                Err(error) if error.code() == CentralErrorCode::ConcurrentUpdate => {
                    let current = self
                        .registry
                        .query_enrollment(&request.tenant_id, &response.enrollment_id)
                        .await?;
                    if current.workload_certificate.as_ref().is_some_and(|winner| {
                        winner.certificate_generation.get() >= generation.get()
                    }) {
                        response.resource_version = current.resource_version;
                        response.certificate = current.workload_certificate;
                        response.validate()?;
                        return Ok(response);
                    }
                    expected_resource_version = current.resource_version;
                    certificate = self.issue_for_record(&current, generation).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(AgentWorkloadCertificateError::ConcurrentUpdate)
    }

    async fn issue_for_record(
        &self,
        record: &AgentRegistryRecord,
        generation: CertificateGeneration,
    ) -> Result<AgentWorkloadCertificateBundle, AgentWorkloadCertificateError> {
        let candidate = record
            .candidate
            .as_ref()
            .ok_or(AgentWorkloadCertificateError::MissingCredentialEvidence)?;
        let evidence = candidate
            .credential_evidence
            .as_ref()
            .ok_or(AgentWorkloadCertificateError::MissingCredentialEvidence)?;
        let identity_uri = WorkloadIdentityUri::agent(
            &self.trust_domain,
            record.enrollment.edge_cluster_id.clone(),
            record.enrollment.reserved_agent_id.clone(),
        )?;
        let now = self.registry.now();
        let request_id_material = format!(
            "{}:{}:{}",
            record.enrollment.enrollment_id,
            record.enrollment.reserved_agent_id,
            generation.get()
        );
        let request_id = RequestId::new(format!(
            "agent-cert-{}",
            ContentDigest::hash(request_id_material.as_bytes())
        ))?;
        let issuance_request = WorkloadCertificateRequest::new(
            request_id,
            identity_uri.clone(),
            generation,
            evidence.public_key_spki.clone(),
            now,
        )?;
        let issued = self.issuance.issue(issuance_request).await?;
        validate_issued_workload_certificate(&issued, now)
            .map_err(|_| AgentWorkloadCertificateError::InvalidCertificateMaterial)?;
        let session_generation = record
            .instance
            .as_ref()
            .and_then(|instance| instance.session_generation)
            .unwrap_or_else(|| SessionGeneration::new(1));
        let bundle = AgentWorkloadCertificateBundle {
            certificate_bundle_version: AgentWorkloadCertificateBundle::VERSION,
            edge_cluster_id: record.enrollment.edge_cluster_id.clone(),
            agent_id: record.enrollment.reserved_agent_id.clone(),
            enrollment_id: record.enrollment.enrollment_id.clone(),
            identity_uri: identity_uri.to_string(),
            public_key_fingerprint: evidence.public_key_spki.fingerprint(),
            certificate_generation: generation,
            session_generation,
            mount_generation: record.mount.mount_generation,
            owner_generation: record.owner.owner_generation,
            not_before_unix_ms: issued.request().not_before_unix_ms(),
            not_after_unix_ms: issued.request().not_after_unix_ms(),
            renew_at_unix_ms: issued.request().renew_at_unix_ms(),
            leaf_certificate_der: AgentWorkloadCertificateDer::new(
                issued.leaf_certificate_der().to_vec(),
            )?,
            issuer_chain_der: issued
                .issuer_chain_der()
                .iter()
                .map(|certificate| AgentWorkloadCertificateDer::new(certificate.clone()))
                .collect::<Result<Vec<_>, _>>()?,
            extensions: Extensions::new(),
        };
        bundle.validate()?;
        Ok(bundle)
    }
}

fn generation_to_issue(
    current: Option<(CertificateGeneration, UnixMillis)>,
    now: UnixMillis,
) -> Result<Option<CertificateGeneration>, AgentWorkloadCertificateError> {
    let Some((generation, renew_at)) = current else {
        return Ok(Some(CertificateGeneration::new(1)));
    };
    if now.get() < renew_at.get() {
        return Ok(None);
    }
    generation
        .get()
        .checked_add(1)
        .map(CertificateGeneration::new)
        .map(Some)
        .ok_or_else(|| {
            AgentWorkloadCertificateError::Registry(CentralError::new(
                CentralErrorCode::AgentIdentityMismatch,
                "Agent workload certificate generation is exhausted",
            ))
        })
}

#[derive(Debug, Error)]
pub enum AgentWorkloadCertificateError {
    #[error("approved Agent has no verified bootstrap credential evidence")]
    MissingCredentialEvidence,
    #[error("issued Agent workload certificate material is invalid")]
    InvalidCertificateMaterial,
    #[error("Agent workload certificate Registry update remained contended")]
    ConcurrentUpdate,
    #[error(transparent)]
    Registry(#[from] CentralError),
    #[error(transparent)]
    Pki(#[from] WorkloadPkiError),
    #[error(transparent)]
    Issuer(#[from] WorkloadCertificateIssuerError),
    #[error(transparent)]
    Protocol(#[from] neoengram_domain::protocol::ProtocolError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_advances_exactly_at_the_half_life_boundary() {
        assert_eq!(
            generation_to_issue(None, UnixMillis::new(100)).unwrap(),
            Some(CertificateGeneration::new(1))
        );
        let current = Some((CertificateGeneration::new(7), UnixMillis::new(500)));
        assert_eq!(
            generation_to_issue(current, UnixMillis::new(499)).unwrap(),
            None
        );
        assert_eq!(
            generation_to_issue(current, UnixMillis::new(500)).unwrap(),
            Some(CertificateGeneration::new(8))
        );
        assert!(generation_to_issue(
            Some((CertificateGeneration::new(u64::MAX), UnixMillis::new(500))),
            UnixMillis::new(500),
        )
        .is_err());
    }
}
