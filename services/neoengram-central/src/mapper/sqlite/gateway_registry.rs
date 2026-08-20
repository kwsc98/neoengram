use async_trait::async_trait;
use neoengram_domain::core::ContentDigest;
use neoengram_domain::protocol::{AgentId, EdgeClusterId, GatewayPoolId, GatewayReplicaId};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{sqlite::SqliteRow, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};

use crate::{
    acquire_route_against, open_agent_session_against, release_route_against, renew_route_against,
    validate_agent_route_owner, validate_agent_route_session, validate_agent_route_target,
    validate_gateway_pool, validate_gateway_pool_replace, validate_gateway_replica,
    validate_gateway_replica_parent_replace, validate_gateway_replica_replace,
    validate_pool_list_request, validate_replica_list_request, validate_route_list_request,
    AcquireAgentRouteLeaseRequest, AcquireAgentSessionRouteRequest, AgentRouteLease,
    AgentRouteLeaseAcquireOutcome, AgentRouteLeaseListRequest, AgentRouteLeaseMutationOutcome,
    AgentSessionRouteAcquireOutcome, CentralError, CentralErrorCode, CentralResult,
    GatewayCredentialState, GatewayInsertOutcome, GatewayPoolListRequest, GatewayPoolRecord,
    GatewayPoolState, GatewayRegistryRepository, GatewayReplicaListRequest, GatewayReplicaRecord,
    GatewayReplicaState, ReleaseAgentRouteLeaseRequest, RenewAgentRouteLeaseRequest,
};

use super::agent_registry::{
    fetch_agent_by_agent_transaction, replace_agent_record_transaction, SqliteAgentRegistryStore,
};

const STORED_FORMAT_VERSION: u32 = 1;
const POOL_COLUMNS: &str = "gateway_pool_id, edge_cluster_id, agent_endpoint, s3_endpoint, state, resource_version, payload";
const REPLICA_COLUMNS: &str = "r.gateway_replica_id, r.gateway_pool_id, r.edge_cluster_id, r.control_endpoint, r.peer_endpoint, r.bootstrap_endpoint, r.state, r.resource_version, r.payload, c.activation_token_digest, c.state, c.certificate_generation, c.payload";
const ROUTE_COLUMNS: &str = "agent_id, edge_cluster_id, gateway_pool_id, gateway_replica_id, connection_id, session_generation, route_generation, acquire_request_id, last_renew_request_id, release_request_id, lease_expires_at_unix_ms, released_at_unix_ms, payload";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredV1<T> {
    format: u32,
    value: T,
}

#[async_trait]
impl GatewayRegistryRepository for SqliteAgentRegistryStore {
    async fn get_pool(
        &self,
        gateway_pool_id: &GatewayPoolId,
    ) -> CentralResult<Option<GatewayPoolRecord>> {
        fetch_pool_by(&self.pool, "gateway_pool_id = ?", gateway_pool_id.as_str()).await
    }

    async fn get_pool_by_edge_cluster(
        &self,
        edge_cluster_id: &EdgeClusterId,
    ) -> CentralResult<Option<GatewayPoolRecord>> {
        fetch_pool_by(&self.pool, "edge_cluster_id = ?", edge_cluster_id.as_str()).await
    }

    async fn list_pools(
        &self,
        request: &GatewayPoolListRequest,
    ) -> CentralResult<Vec<GatewayPoolRecord>> {
        validate_pool_list_request(request)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {POOL_COLUMNS} FROM gateway_pool_records WHERE 1 = 1"
        ));
        if let Some(cluster) = &request.edge_cluster_id {
            query
                .push(" AND edge_cluster_id = ")
                .push_bind(cluster.as_str());
        }
        if let Some(state) = request.state {
            query.push(" AND state = ").push_bind(pool_state(state));
        }
        if let Some(after) = &request.after {
            query
                .push(" AND gateway_pool_id > ")
                .push_bind(after.as_str());
        }
        query
            .push(" ORDER BY gateway_pool_id ASC LIMIT ")
            .push_bind(as_i64_usize(request.limit)?);
        query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_pool_row)
            .collect()
    }

    async fn insert_pool(
        &self,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayPoolRecord>> {
        validate_gateway_pool(&record)?;
        if record.resource_version.get() != 1 || record.config_generation.get() != 1 {
            return invalid("new GatewayPool must begin at ResourceVersion/config generation 1");
        }
        if let Some(existing) = self.get_pool(&record.gateway_pool_id).await? {
            return if existing == record {
                Ok(GatewayInsertOutcome::Existing(existing))
            } else {
                identity_conflict("GatewayPool ID is already used")
            };
        }
        if self
            .get_pool_by_edge_cluster(&record.edge_cluster_id)
            .await?
            .is_some()
        {
            return identity_conflict("EdgeCluster already has a GatewayPool");
        }
        sqlx::query(
            "INSERT INTO gateway_pool_records \
             (gateway_pool_id, edge_cluster_id, agent_endpoint, s3_endpoint, state, \
              resource_version, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.gateway_pool_id.as_str())
        .bind(record.edge_cluster_id.as_str())
        .bind(&record.agent_endpoint)
        .bind(&record.s3_endpoint)
        .bind(pool_state(record.state))
        .bind(record.resource_version.to_string())
        .bind(encode(&record)?)
        .execute(&self.pool)
        .await
        .map_err(map_identity_write)?;
        Ok(GatewayInsertOutcome::Inserted(record))
    }

    async fn replace_pool(
        &self,
        expected_resource_version: u64,
        record: GatewayPoolRecord,
    ) -> CentralResult<GatewayPoolRecord> {
        let stored = self
            .get_pool(&record.gateway_pool_id)
            .await?
            .ok_or_else(pool_not_found)?;
        validate_gateway_pool_replace(&stored, expected_resource_version, &record)?;
        let result = sqlx::query(
            "UPDATE gateway_pool_records SET agent_endpoint = ?, s3_endpoint = ?, state = ?, \
             resource_version = ?, payload = ? \
             WHERE gateway_pool_id = ? AND resource_version = ?",
        )
        .bind(&record.agent_endpoint)
        .bind(&record.s3_endpoint)
        .bind(pool_state(record.state))
        .bind(record.resource_version.to_string())
        .bind(encode(&record)?)
        .bind(record.gateway_pool_id.as_str())
        .bind(expected_resource_version.to_string())
        .execute(&self.pool)
        .await
        .map_err(map_identity_write)?;
        if result.rows_affected() != 1 {
            return concurrent("GatewayPool ResourceVersion changed");
        }
        Ok(record)
    }

    async fn get_replica(
        &self,
        gateway_replica_id: &GatewayReplicaId,
    ) -> CentralResult<Option<GatewayReplicaRecord>> {
        fetch_replica_by(
            &self.pool,
            "r.gateway_replica_id = ?",
            gateway_replica_id.as_str(),
        )
        .await
    }

    async fn get_replica_by_activation_token_digest(
        &self,
        token_digest: &ContentDigest,
    ) -> CentralResult<Option<GatewayReplicaRecord>> {
        let row = sqlx::query(&format!(
            "SELECT {REPLICA_COLUMNS} FROM gateway_replica_records r \
             JOIN gateway_replica_credentials c USING (gateway_replica_id) \
             WHERE c.activation_token_digest = ?"
        ))
        .bind(token_digest.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_replica_row).transpose()
    }

    async fn list_replicas(
        &self,
        request: &GatewayReplicaListRequest,
    ) -> CentralResult<Vec<GatewayReplicaRecord>> {
        validate_replica_list_request(request)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {REPLICA_COLUMNS} FROM gateway_replica_records r \
             JOIN gateway_replica_credentials c USING (gateway_replica_id) \
             WHERE r.gateway_pool_id = "
        ));
        query.push_bind(request.gateway_pool_id.as_str());
        if let Some(state) = request.state {
            query
                .push(" AND r.state = ")
                .push_bind(replica_state(state));
        }
        if let Some(after) = &request.after {
            query
                .push(" AND r.gateway_replica_id > ")
                .push_bind(after.as_str());
        }
        query
            .push(" ORDER BY r.gateway_replica_id ASC LIMIT ")
            .push_bind(as_i64_usize(request.limit)?);
        query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_replica_row)
            .collect()
    }

    async fn insert_replica(
        &self,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayInsertOutcome<GatewayReplicaRecord>> {
        validate_gateway_replica(&record)?;
        if record.resource_version.get() != 1
            || record.state != GatewayReplicaState::Pending
            || record.credential.state != GatewayCredentialState::PendingActivation
            || record.last_heartbeat_at_unix_ms.is_some()
        {
            return invalid("new GatewayReplica must begin pending at ResourceVersion 1");
        }
        // Keep identity, pool-state, endpoint, and both row inserts in one transaction.  Reading
        // the Pool before BEGIN allowed a concurrent disable to race with the subsequent insert;
        // SQLite's FK only checks Pool existence, not its lifecycle state.
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        if let Some(existing) =
            fetch_replica_transaction(&mut transaction, &record.gateway_replica_id).await?
        {
            transaction.rollback().await.map_err(storage_error)?;
            return if existing == record {
                Ok(GatewayInsertOutcome::Existing(existing))
            } else {
                identity_conflict("GatewayReplica ID is already used")
            };
        }
        let pool = fetch_pool_transaction(&mut transaction, &record.gateway_pool_id)
            .await?
            .ok_or_else(pool_not_found)?;
        if pool.edge_cluster_id != record.edge_cluster_id
            || matches!(
                pool.state,
                GatewayPoolState::Draining | GatewayPoolState::Disabled
            )
        {
            transaction.rollback().await.map_err(storage_error)?;
            return identity_conflict("GatewayReplica scope differs from its GatewayPool");
        }
        ensure_replica_endpoints_unique(&mut transaction, &record).await?;
        sqlx::query(
            "INSERT INTO gateway_replica_records \
             (gateway_replica_id, gateway_pool_id, edge_cluster_id, control_endpoint, \
              peer_endpoint, bootstrap_endpoint, state, resource_version, payload) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(record.gateway_replica_id.as_str())
        .bind(record.gateway_pool_id.as_str())
        .bind(record.edge_cluster_id.as_str())
        .bind(&record.control_endpoint)
        .bind(&record.peer_endpoint)
        .bind(&record.bootstrap_endpoint)
        .bind(replica_state(record.state))
        .bind(record.resource_version.to_string())
        .bind(encode(&record)?)
        .execute(&mut *transaction)
        .await
        .map_err(map_identity_write)?;
        insert_credential(&mut transaction, &record).await?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(GatewayInsertOutcome::Inserted(record))
    }

    async fn replace_replica(
        &self,
        expected_resource_version: u64,
        record: GatewayReplicaRecord,
    ) -> CentralResult<GatewayReplicaRecord> {
        // Validate the stored Replica and its Pool from the same snapshot that performs the
        // update.  Otherwise a concurrent Pool disable can be observed after validation and still
        // permit this replacement to commit.
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let stored = fetch_replica_transaction(&mut transaction, &record.gateway_replica_id)
            .await?
            .ok_or_else(replica_not_found)?;
        validate_gateway_replica_replace(&stored, expected_resource_version, &record)?;
        let pool = fetch_pool_transaction(&mut transaction, &record.gateway_pool_id)
            .await?
            .ok_or_else(pool_not_found)?;
        validate_gateway_replica_parent_replace(&pool, &stored, &record)?;
        ensure_replica_endpoints_unique(&mut transaction, &record).await?;
        let result = sqlx::query(
            "UPDATE gateway_replica_records SET control_endpoint = ?, peer_endpoint = ?, \
             bootstrap_endpoint = ?, state = ?, resource_version = ?, payload = ? \
             WHERE gateway_replica_id = ? AND resource_version = ?",
        )
        .bind(&record.control_endpoint)
        .bind(&record.peer_endpoint)
        .bind(&record.bootstrap_endpoint)
        .bind(replica_state(record.state))
        .bind(record.resource_version.to_string())
        .bind(encode(&record)?)
        .bind(record.gateway_replica_id.as_str())
        .bind(expected_resource_version.to_string())
        .execute(&mut *transaction)
        .await
        .map_err(map_identity_write)?;
        if result.rows_affected() != 1 {
            return concurrent("GatewayReplica ResourceVersion changed");
        }
        sqlx::query(
            "UPDATE gateway_replica_credentials SET state = ?, certificate_generation = ?, \
             payload = ? WHERE gateway_replica_id = ?",
        )
        .bind(credential_state(record.credential.state))
        .bind(
            record
                .credential
                .certificate_generation
                .map(|generation| generation.to_string()),
        )
        .bind(encode(&record.credential)?)
        .bind(record.gateway_replica_id.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(storage_error)?;
        transaction.commit().await.map_err(storage_error)?;
        Ok(record)
    }

    async fn get_agent_route(&self, agent_id: &AgentId) -> CentralResult<Option<AgentRouteLease>> {
        let row = sqlx::query(&format!(
            "SELECT {ROUTE_COLUMNS} FROM agent_route_leases WHERE agent_id = ?"
        ))
        .bind(agent_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;
        row.map(decode_route_row).transpose()
    }

    async fn list_agent_routes(
        &self,
        request: &AgentRouteLeaseListRequest,
    ) -> CentralResult<Vec<AgentRouteLease>> {
        validate_route_list_request(request)?;
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "SELECT {ROUTE_COLUMNS} FROM agent_route_leases WHERE gateway_pool_id = "
        ));
        query.push_bind(request.gateway_pool_id.as_str());
        if let Some(replica) = &request.gateway_replica_id {
            query
                .push(" AND gateway_replica_id = ")
                .push_bind(replica.as_str());
        }
        if let Some(now) = request.active_at_unix_ms {
            query
                .push(" AND released_at_unix_ms IS NULL AND lease_expires_at_unix_ms > ")
                .push_bind(as_i64(now.get())?);
        }
        if let Some(after) = &request.after {
            query.push(" AND agent_id > ").push_bind(after.as_str());
        }
        query
            .push(" ORDER BY agent_id ASC LIMIT ")
            .push_bind(as_i64_usize(request.limit)?);
        query
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error)?
            .into_iter()
            .map(decode_route_row)
            .collect()
    }

    async fn acquire_agent_route(
        &self,
        request: AcquireAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseAcquireOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let agent = fetch_agent_by_agent_transaction(&mut transaction, &request.agent_id)
            .await?
            .ok_or_else(agent_not_found)?;
        validate_agent_route_target(&agent, &request)?;
        require_route_target(&mut transaction, &request).await?;
        ensure_route_identities_available(
            &mut transaction,
            &request.agent_id,
            &request.connection_id,
            &request.request_id,
        )
        .await?;
        let stored = fetch_route_transaction(&mut transaction, &request.agent_id).await?;
        let outcome = acquire_route_against(stored.as_ref(), &request)?;
        if !outcome.replayed {
            upsert_route(&mut transaction, &outcome.lease).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn acquire_agent_session_route(
        &self,
        request: AcquireAgentSessionRouteRequest,
    ) -> CentralResult<AgentSessionRouteAcquireOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let stored_agent =
            fetch_agent_by_agent_transaction(&mut transaction, &request.session.agent_id)
                .await?
                .ok_or_else(agent_not_found)?;
        let session = open_agent_session_against(
            stored_agent.clone(),
            &request.session,
            request.observed_at_unix_ms,
            request.heartbeat_timeout_ms,
        )?;
        let route_request = request.route_request(&session);
        validate_agent_route_target(&session.record, &route_request)?;
        require_route_target(&mut transaction, &route_request).await?;
        ensure_route_identities_available(
            &mut transaction,
            &route_request.agent_id,
            &route_request.connection_id,
            &route_request.request_id,
        )
        .await?;
        let stored_route =
            fetch_route_transaction(&mut transaction, &route_request.agent_id).await?;
        let route = acquire_route_against(stored_route.as_ref(), &route_request)?;
        if !session.replayed {
            replace_agent_record_transaction(
                &mut transaction,
                &stored_agent,
                request.session.expected_resource_version.get(),
                &session.record,
            )
            .await?;
        }
        if !route.replayed {
            upsert_route(&mut transaction, &route.lease).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(AgentSessionRouteAcquireOutcome { session, route })
    }

    async fn renew_agent_route(
        &self,
        request: RenewAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let agent = fetch_agent_by_agent_transaction(&mut transaction, &request.agent_id)
            .await?
            .ok_or_else(agent_not_found)?;
        validate_agent_route_session(&agent, request.agent_id.clone(), request.session_generation)?;
        ensure_request_available(&mut transaction, &request.agent_id, &request.request_id).await?;
        let stored = fetch_route_transaction(&mut transaction, &request.agent_id)
            .await?
            .ok_or_else(route_not_found)?;
        let pool = fetch_pool_transaction(&mut transaction, &stored.gateway_pool_id)
            .await?
            .ok_or_else(pool_not_found)?;
        let replica = fetch_replica_transaction(&mut transaction, &stored.gateway_replica_id)
            .await?
            .ok_or_else(replica_not_found)?;
        validate_agent_route_owner(
            &pool,
            &replica,
            &stored.edge_cluster_id,
            &stored.gateway_pool_id,
            &stored.gateway_replica_id,
            request.renewed_at_unix_ms,
        )?;
        let outcome = renew_route_against(&stored, &request)?;
        if !outcome.replayed {
            upsert_route(&mut transaction, &outcome.lease).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }

    async fn release_agent_route(
        &self,
        request: ReleaseAgentRouteLeaseRequest,
    ) -> CentralResult<AgentRouteLeaseMutationOutcome> {
        let mut transaction = self.pool.begin().await.map_err(storage_error)?;
        let agent = fetch_agent_by_agent_transaction(&mut transaction, &request.agent_id)
            .await?
            .ok_or_else(agent_not_found)?;
        validate_agent_route_session(&agent, request.agent_id.clone(), request.session_generation)?;
        ensure_request_available(&mut transaction, &request.agent_id, &request.request_id).await?;
        let stored = fetch_route_transaction(&mut transaction, &request.agent_id)
            .await?
            .ok_or_else(route_not_found)?;
        let outcome = release_route_against(&stored, &request)?;
        if !outcome.replayed {
            upsert_route(&mut transaction, &outcome.lease).await?;
        }
        transaction.commit().await.map_err(storage_error)?;
        Ok(outcome)
    }
}

async fn fetch_pool_by(
    pool: &SqlitePool,
    predicate: &str,
    value: &str,
) -> CentralResult<Option<GatewayPoolRecord>> {
    let row = sqlx::query(&format!(
        "SELECT {POOL_COLUMNS} FROM gateway_pool_records WHERE {predicate}"
    ))
    .bind(value)
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    row.map(decode_pool_row).transpose()
}

async fn fetch_pool_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    gateway_pool_id: &GatewayPoolId,
) -> CentralResult<Option<GatewayPoolRecord>> {
    let row = sqlx::query(&format!(
        "SELECT {POOL_COLUMNS} FROM gateway_pool_records WHERE gateway_pool_id = ?"
    ))
    .bind(gateway_pool_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(decode_pool_row).transpose()
}

async fn fetch_replica_by(
    pool: &SqlitePool,
    predicate: &str,
    value: &str,
) -> CentralResult<Option<GatewayReplicaRecord>> {
    let row = sqlx::query(&format!(
        "SELECT {REPLICA_COLUMNS} FROM gateway_replica_records r \
         JOIN gateway_replica_credentials c USING (gateway_replica_id) WHERE {predicate}"
    ))
    .bind(value)
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    row.map(decode_replica_row).transpose()
}

async fn fetch_replica_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    gateway_replica_id: &GatewayReplicaId,
) -> CentralResult<Option<GatewayReplicaRecord>> {
    let row = sqlx::query(&format!(
        "SELECT {REPLICA_COLUMNS} FROM gateway_replica_records r \
         JOIN gateway_replica_credentials c USING (gateway_replica_id) \
         WHERE r.gateway_replica_id = ?"
    ))
    .bind(gateway_replica_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(decode_replica_row).transpose()
}

async fn insert_credential(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &GatewayReplicaRecord,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO gateway_replica_credentials \
         (gateway_replica_id, activation_token_digest, state, certificate_generation, payload) \
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(record.gateway_replica_id.as_str())
    .bind(
        record
            .credential
            .activation_token_digest
            .as_bytes()
            .as_slice(),
    )
    .bind(credential_state(record.credential.state))
    .bind(
        record
            .credential
            .certificate_generation
            .map(|generation| generation.to_string()),
    )
    .bind(encode(&record.credential)?)
    .execute(&mut **transaction)
    .await
    .map_err(map_identity_write)?;
    Ok(())
}

async fn ensure_replica_endpoints_unique(
    transaction: &mut Transaction<'_, Sqlite>,
    record: &GatewayReplicaRecord,
) -> CentralResult<()> {
    // The SQLite schema has per-column UNIQUE constraints, but those do not prevent a control
    // endpoint from being reused as another Replica's peer/bootstrap endpoint. Keep the full
    // three-role check inside the existing single-connection transaction so insert and replace
    // remain atomic without introducing a second endpoint-claims schema/table.
    let conflict: Option<String> = sqlx::query_scalar(
        "SELECT gateway_replica_id FROM gateway_replica_records \
         WHERE gateway_replica_id <> ? \
           AND (control_endpoint IN (?, ?, ?) \
                OR peer_endpoint IN (?, ?, ?) \
                OR bootstrap_endpoint IN (?, ?, ?)) \
         LIMIT 1",
    )
    .bind(record.gateway_replica_id.as_str())
    .bind(&record.control_endpoint)
    .bind(&record.peer_endpoint)
    .bind(&record.bootstrap_endpoint)
    .bind(&record.control_endpoint)
    .bind(&record.peer_endpoint)
    .bind(&record.bootstrap_endpoint)
    .bind(&record.control_endpoint)
    .bind(&record.peer_endpoint)
    .bind(&record.bootstrap_endpoint)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if conflict.is_some() {
        return identity_conflict(
            "GatewayReplica endpoint is already registered under another Replica",
        );
    }
    Ok(())
}

async fn require_route_target(
    transaction: &mut Transaction<'_, Sqlite>,
    request: &AcquireAgentRouteLeaseRequest,
) -> CentralResult<()> {
    let pool = fetch_pool_transaction(transaction, &request.gateway_pool_id)
        .await?
        .ok_or_else(pool_not_found)?;
    let replica = fetch_replica_transaction(transaction, &request.gateway_replica_id)
        .await?
        .ok_or_else(replica_not_found)?;
    validate_agent_route_owner(
        &pool,
        &replica,
        &request.edge_cluster_id,
        &request.gateway_pool_id,
        &request.gateway_replica_id,
        request.acquired_at_unix_ms,
    )
}

async fn ensure_route_identities_available(
    transaction: &mut Transaction<'_, Sqlite>,
    agent_id: &AgentId,
    connection_id: &neoengram_domain::protocol::GatewayConnectionId,
    request_id: &neoengram_domain::protocol::RequestId,
) -> CentralResult<()> {
    let conflicting_connection: Option<String> = sqlx::query_scalar(
        "SELECT agent_id FROM agent_route_leases WHERE connection_id = ? AND agent_id <> ?",
    )
    .bind(connection_id.as_str())
    .bind(agent_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if conflicting_connection.is_some() {
        return identity_conflict("Gateway connection ID is already bound to another Agent");
    }
    ensure_request_available(transaction, agent_id, request_id).await
}

async fn ensure_request_available(
    transaction: &mut Transaction<'_, Sqlite>,
    agent_id: &AgentId,
    request_id: &neoengram_domain::protocol::RequestId,
) -> CentralResult<()> {
    let conflicting: Option<String> = sqlx::query_scalar(
        "SELECT agent_id FROM agent_route_leases \
         WHERE agent_id <> ? AND (acquire_request_id = ? OR last_renew_request_id = ? \
                                  OR release_request_id = ?)",
    )
    .bind(agent_id.as_str())
    .bind(request_id.as_str())
    .bind(request_id.as_str())
    .bind(request_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    if conflicting.is_some() {
        return identity_conflict("Gateway route RequestId is already bound to another Agent");
    }
    Ok(())
}

async fn fetch_route_transaction(
    transaction: &mut Transaction<'_, Sqlite>,
    agent_id: &AgentId,
) -> CentralResult<Option<AgentRouteLease>> {
    let row = sqlx::query(&format!(
        "SELECT {ROUTE_COLUMNS} FROM agent_route_leases WHERE agent_id = ?"
    ))
    .bind(agent_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(storage_error)?;
    row.map(decode_route_row).transpose()
}

async fn upsert_route(
    transaction: &mut Transaction<'_, Sqlite>,
    lease: &AgentRouteLease,
) -> CentralResult<()> {
    sqlx::query(
        "INSERT INTO agent_route_leases \
         (agent_id, edge_cluster_id, gateway_pool_id, gateway_replica_id, connection_id, \
          session_generation, route_generation, acquire_request_id, last_renew_request_id, \
          release_request_id, lease_expires_at_unix_ms, released_at_unix_ms, payload) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(agent_id) DO UPDATE SET \
          edge_cluster_id = excluded.edge_cluster_id, \
          gateway_pool_id = excluded.gateway_pool_id, \
          gateway_replica_id = excluded.gateway_replica_id, \
          connection_id = excluded.connection_id, \
          session_generation = excluded.session_generation, \
          route_generation = excluded.route_generation, \
          acquire_request_id = excluded.acquire_request_id, \
          last_renew_request_id = excluded.last_renew_request_id, \
          release_request_id = excluded.release_request_id, \
          lease_expires_at_unix_ms = excluded.lease_expires_at_unix_ms, \
          released_at_unix_ms = excluded.released_at_unix_ms, \
          payload = excluded.payload",
    )
    .bind(lease.agent_id.as_str())
    .bind(lease.edge_cluster_id.as_str())
    .bind(lease.gateway_pool_id.as_str())
    .bind(lease.gateway_replica_id.as_str())
    .bind(lease.connection_id.as_str())
    .bind(lease.session_generation.to_string())
    .bind(lease.route_generation.to_string())
    .bind(lease.acquire_request_id.as_str())
    .bind(
        lease
            .last_renew_request_id
            .as_ref()
            .map(neoengram_domain::protocol::RequestId::as_str),
    )
    .bind(
        lease
            .release_request_id
            .as_ref()
            .map(neoengram_domain::protocol::RequestId::as_str),
    )
    .bind(as_i64(lease.lease_expires_at_unix_ms.get())?)
    .bind(
        lease
            .released_at_unix_ms
            .map(|timestamp| as_i64(timestamp.get()))
            .transpose()?,
    )
    .bind(encode(lease)?)
    .execute(&mut **transaction)
    .await
    .map_err(map_identity_write)?;
    Ok(())
}

fn decode_pool_row(row: SqliteRow) -> CentralResult<GatewayPoolRecord> {
    let record: GatewayPoolRecord = decode(row.try_get("payload").map_err(storage_error)?)?;
    validate_gateway_pool(&record).map_err(as_corruption)?;
    let indexed = (
        row.try_get::<String, _>("gateway_pool_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("edge_cluster_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("agent_endpoint")
            .map_err(storage_error)?,
        row.try_get::<Option<String>, _>("s3_endpoint")
            .map_err(storage_error)?,
        row.try_get::<String, _>("state").map_err(storage_error)?,
        row.try_get::<String, _>("resource_version")
            .map_err(storage_error)?,
    );
    let expected = (
        record.gateway_pool_id.to_string(),
        record.edge_cluster_id.to_string(),
        record.agent_endpoint.clone(),
        record.s3_endpoint.clone(),
        pool_state(record.state).to_owned(),
        record.resource_version.to_string(),
    );
    if indexed != expected {
        return Err(storage_corruption(
            "GatewayPool indexed columns disagree with its payload",
        ));
    }
    Ok(record)
}

fn decode_replica_row(row: SqliteRow) -> CentralResult<GatewayReplicaRecord> {
    let record: GatewayReplicaRecord = decode(
        row.try_get::<Vec<u8>, _>(8)
            .map_err(storage_error)?
            .as_slice(),
    )?;
    let credential: crate::GatewayReplicaCredential = decode(
        row.try_get::<Vec<u8>, _>(12)
            .map_err(storage_error)?
            .as_slice(),
    )?;
    validate_gateway_replica(&record).map_err(as_corruption)?;
    if credential != record.credential {
        return Err(storage_corruption(
            "GatewayReplica credential payloads disagree",
        ));
    }
    let token_digest: Vec<u8> = row.try_get(9).map_err(storage_error)?;
    let certificate_generation: Option<String> = row.try_get(11).map_err(storage_error)?;
    let indexed = (
        row.try_get::<String, _>(0).map_err(storage_error)?,
        row.try_get::<String, _>(1).map_err(storage_error)?,
        row.try_get::<String, _>(2).map_err(storage_error)?,
        row.try_get::<String, _>(3).map_err(storage_error)?,
        row.try_get::<String, _>(4).map_err(storage_error)?,
        row.try_get::<String, _>(5).map_err(storage_error)?,
        row.try_get::<String, _>(6).map_err(storage_error)?,
        row.try_get::<String, _>(7).map_err(storage_error)?,
        token_digest,
        row.try_get::<String, _>(10).map_err(storage_error)?,
        certificate_generation,
    );
    let expected = (
        record.gateway_replica_id.to_string(),
        record.gateway_pool_id.to_string(),
        record.edge_cluster_id.to_string(),
        record.control_endpoint.clone(),
        record.peer_endpoint.clone(),
        record.bootstrap_endpoint.clone(),
        replica_state(record.state).to_owned(),
        record.resource_version.to_string(),
        record
            .credential
            .activation_token_digest
            .as_bytes()
            .to_vec(),
        credential_state(record.credential.state).to_owned(),
        record
            .credential
            .certificate_generation
            .map(|generation| generation.to_string()),
    );
    if indexed != expected {
        return Err(storage_corruption(
            "GatewayReplica indexed columns disagree with its payload",
        ));
    }
    Ok(record)
}

fn decode_route_row(row: SqliteRow) -> CentralResult<AgentRouteLease> {
    let lease: AgentRouteLease = decode(row.try_get("payload").map_err(storage_error)?)?;
    let indexed = (
        row.try_get::<String, _>("agent_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("edge_cluster_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("gateway_pool_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("gateway_replica_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("connection_id")
            .map_err(storage_error)?,
        row.try_get::<String, _>("session_generation")
            .map_err(storage_error)?,
        row.try_get::<String, _>("route_generation")
            .map_err(storage_error)?,
        row.try_get::<String, _>("acquire_request_id")
            .map_err(storage_error)?,
        row.try_get::<Option<String>, _>("last_renew_request_id")
            .map_err(storage_error)?,
        row.try_get::<Option<String>, _>("release_request_id")
            .map_err(storage_error)?,
        row.try_get::<i64, _>("lease_expires_at_unix_ms")
            .map_err(storage_error)?,
        row.try_get::<Option<i64>, _>("released_at_unix_ms")
            .map_err(storage_error)?,
    );
    let expected = (
        lease.agent_id.to_string(),
        lease.edge_cluster_id.to_string(),
        lease.gateway_pool_id.to_string(),
        lease.gateway_replica_id.to_string(),
        lease.connection_id.to_string(),
        lease.session_generation.to_string(),
        lease.route_generation.to_string(),
        lease.acquire_request_id.to_string(),
        lease
            .last_renew_request_id
            .as_ref()
            .map(ToString::to_string),
        lease.release_request_id.as_ref().map(ToString::to_string),
        as_i64(lease.lease_expires_at_unix_ms.get())?,
        lease
            .released_at_unix_ms
            .map(|timestamp| as_i64(timestamp.get()))
            .transpose()?,
    );
    if indexed != expected
        || lease.route_generation.get() == 0
        || lease.session_generation.get() == 0
        || lease.acquired_lease_expires_at_unix_ms.get() <= lease.acquired_at_unix_ms.get()
        || lease
            .acquired_lease_expires_at_unix_ms
            .get()
            .saturating_sub(lease.acquired_at_unix_ms.get())
            > crate::AGENT_ROUTE_LEASE_MAX_TTL_MS
        || lease.renewed_at_unix_ms.get() < lease.acquired_at_unix_ms.get()
        || lease.lease_expires_at_unix_ms.get() < lease.acquired_at_unix_ms.get()
        || lease
            .released_at_unix_ms
            .is_some_and(|released| released.get() < lease.renewed_at_unix_ms.get())
        || (lease.released_at_unix_ms.is_none()
            && (lease.lease_expires_at_unix_ms.get() <= lease.renewed_at_unix_ms.get()
                || lease
                    .lease_expires_at_unix_ms
                    .get()
                    .saturating_sub(lease.renewed_at_unix_ms.get())
                    > crate::AGENT_ROUTE_LEASE_MAX_TTL_MS))
    {
        return Err(storage_corruption(
            "AgentRouteLease indexed columns or payload are invalid",
        ));
    }
    Ok(lease)
}

pub(super) async fn validate_gateway_records(pool: &SqlitePool) -> CentralResult<()> {
    for row in sqlx::query(&format!(
        "SELECT {POOL_COLUMNS} FROM gateway_pool_records ORDER BY gateway_pool_id"
    ))
    .fetch_all(pool)
    .await
    .map_err(storage_error)?
    {
        decode_pool_row(row)?;
    }
    for row in sqlx::query(&format!(
        "SELECT {REPLICA_COLUMNS} FROM gateway_replica_records r \
         JOIN gateway_replica_credentials c USING (gateway_replica_id) \
         ORDER BY r.gateway_replica_id"
    ))
    .fetch_all(pool)
    .await
    .map_err(storage_error)?
    {
        decode_replica_row(row)?;
    }
    let duplicate_endpoint: Option<String> = sqlx::query_scalar(
        "SELECT endpoint FROM (\
             SELECT control_endpoint AS endpoint FROM gateway_replica_records \
             UNION ALL \
             SELECT peer_endpoint AS endpoint FROM gateway_replica_records \
             UNION ALL \
             SELECT bootstrap_endpoint AS endpoint FROM gateway_replica_records\
         ) GROUP BY endpoint HAVING count(*) > 1 LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    if duplicate_endpoint.is_some() {
        return Err(storage_corruption(
            "GatewayReplica endpoints are not globally unique",
        ));
    }
    // SQLite can enforce uniqueness within each request-id column, but not across the acquire,
    // renew, and release columns.  The write path rejects cross-column reuse; repeat that check at
    // startup so a manually edited or older database cannot make one idempotency key ambiguous.
    let duplicate_route_request_id: Option<String> = sqlx::query_scalar(
        "SELECT request_id FROM (\
             SELECT acquire_request_id AS request_id FROM agent_route_leases \
             UNION ALL \
             SELECT last_renew_request_id AS request_id FROM agent_route_leases \
             UNION ALL \
             SELECT release_request_id AS request_id FROM agent_route_leases\
         ) WHERE request_id IS NOT NULL \
         GROUP BY request_id HAVING count(*) > 1 LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .map_err(storage_error)?;
    if duplicate_route_request_id.is_some() {
        return Err(storage_corruption(
            "AgentRouteLease mutation RequestIds are not globally unique",
        ));
    }
    for row in sqlx::query(&format!(
        "SELECT {ROUTE_COLUMNS} FROM agent_route_leases ORDER BY agent_id"
    ))
    .fetch_all(pool)
    .await
    .map_err(storage_error)?
    {
        decode_route_row(row)?;
    }
    // Credentials are a required one-to-one extension of every Replica. The INNER JOIN used by
    // normal reads cannot reveal a Replica whose credential row was lost, so check that direction
    // explicitly instead of silently hiding an unusable identity at startup.
    let replicas_without_credentials: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gateway_replica_records r \
         LEFT JOIN gateway_replica_credentials c USING (gateway_replica_id) \
         WHERE c.gateway_replica_id IS NULL",
    )
    .fetch_one(pool)
    .await
    .map_err(storage_error)?;
    if replicas_without_credentials != 0 {
        return Err(storage_corruption(
            "Gateway registry contains a Replica without credentials",
        ));
    }
    let orphan_credentials: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM gateway_replica_credentials c \
         LEFT JOIN gateway_replica_records r USING (gateway_replica_id) \
         WHERE r.gateway_replica_id IS NULL",
    )
    .fetch_one(pool)
    .await
    .map_err(storage_error)?;
    if orphan_credentials != 0 {
        return Err(storage_corruption(
            "Gateway registry contains orphan credentials",
        ));
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> CentralResult<Vec<u8>> {
    serde_json::to_vec(&StoredV1 {
        format: STORED_FORMAT_VERSION,
        value,
    })
    .map_err(|error| storage_corruption(format!("failed to encode Gateway record: {error}")))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> CentralResult<T> {
    let stored: StoredV1<T> = serde_json::from_slice(bytes).map_err(|error| {
        storage_corruption(format!("Gateway record is not valid current JSON: {error}"))
    })?;
    if stored.format != STORED_FORMAT_VERSION {
        return Err(storage_corruption(format!(
            "Gateway record format {} is unsupported",
            stored.format
        )));
    }
    Ok(stored.value)
}

fn pool_state(state: GatewayPoolState) -> &'static str {
    match state {
        GatewayPoolState::Provisioning => "provisioning",
        GatewayPoolState::Ready => "ready",
        GatewayPoolState::Draining => "draining",
        GatewayPoolState::Disabled => "disabled",
    }
}

fn replica_state(state: GatewayReplicaState) -> &'static str {
    match state {
        GatewayReplicaState::Pending => "pending",
        GatewayReplicaState::Active => "active",
        GatewayReplicaState::Draining => "draining",
        GatewayReplicaState::Revoked => "revoked",
    }
}

fn credential_state(state: GatewayCredentialState) -> &'static str {
    match state {
        GatewayCredentialState::PendingActivation => "pending_activation",
        GatewayCredentialState::PendingCertificateDelivery => "pending_certificate_delivery",
        GatewayCredentialState::Active => "active",
        GatewayCredentialState::Expired => "expired",
        GatewayCredentialState::Revoked => "revoked",
    }
}

fn as_i64(value: u64) -> CentralResult<i64> {
    i64::try_from(value).map_err(|_| storage_corruption("Gateway timestamp exceeds SQLite i64"))
}

fn as_i64_usize(value: usize) -> CentralResult<i64> {
    i64::try_from(value).map_err(|_| storage_corruption("Gateway list limit exceeds SQLite i64"))
}

fn as_corruption(error: CentralError) -> CentralError {
    storage_corruption(error.to_string())
}

fn map_identity_write(error: sqlx::Error) -> CentralError {
    if error
        .as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
    {
        CentralError::new(
            CentralErrorCode::GatewayIdentityConflict,
            "Gateway identity or endpoint is already registered",
        )
        .with_retryable(false)
    } else {
        storage_error(error)
    }
}

fn storage_error(error: impl std::fmt::Display) -> CentralError {
    storage_corruption(error.to_string())
}

fn storage_corruption(message: impl Into<String>) -> CentralError {
    CentralError::new(CentralErrorCode::StorageFailure, message)
}

fn pool_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayPoolNotFound,
        "GatewayPool does not exist",
    )
    .with_retryable(false)
}

fn replica_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayReplicaNotFound,
        "GatewayReplica does not exist",
    )
    .with_retryable(false)
}

fn agent_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayRouteUnavailable,
        "Agent route requires a registered Agent",
    )
    .with_retryable(false)
}

fn route_not_found() -> CentralError {
    CentralError::new(
        CentralErrorCode::GatewayRouteUnavailable,
        "Agent route lease does not exist",
    )
}

fn invalid<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::InvalidState, message).with_retryable(false))
}

fn identity_conflict<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(CentralErrorCode::GatewayIdentityConflict, message).with_retryable(false))
}

fn concurrent<T>(message: impl Into<String>) -> CentralResult<T> {
    Err(CentralError::new(
        CentralErrorCode::ConcurrentUpdate,
        message,
    ))
}
