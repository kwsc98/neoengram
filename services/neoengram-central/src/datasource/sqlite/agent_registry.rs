/// Tables owned by the Agent Registry, catalog, Gateway registry and lifecycle contexts.
///
/// This is only a schema fragment. Runtime connections and schema identity validation live in
/// [`super::authority::SqliteAuthorityDataSource`], so Central has exactly one SQLite authority
/// entry point and one transaction boundary.
pub(crate) const SCHEMA_SQL: &str = r#"
CREATE TABLE agent_registry_records (
    enrollment_id TEXT NOT NULL PRIMARY KEY,
    agent_id TEXT NOT NULL UNIQUE,
    token_id TEXT NOT NULL UNIQUE,
    token_request_id TEXT NOT NULL,
    token_digest TEXT NOT NULL UNIQUE,
    tenant_id TEXT NOT NULL,
    edge_cluster_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    pvc_identity_digest TEXT NOT NULL,
    pvc_binding_role TEXT CHECK (pvc_binding_role IN ('owner', 'replacement')),
    bootstrap_request_id TEXT,
    decision_request_id TEXT,
    installation_id TEXT,
    public_key_fingerprint TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    enrollment_state TEXT NOT NULL DEFAULT 'token_issued' CHECK (
        enrollment_state IN (
            'token_issued', 'pending_approval', 'approved', 'enrolled',
            'rejected', 'expired', 'revoked'
        )
    ),
    registration_kind TEXT NOT NULL DEFAULT 'initial' CHECK (
        registration_kind IN ('initial', 'replacement')
    ),
    enrollment_created_at_unix_ms INTEGER NOT NULL DEFAULT 0 CHECK (
        enrollment_created_at_unix_ms >= 0
    ),
    display_name TEXT
) STRICT;
CREATE TABLE agent_bootstrap_status_watermarks (
    enrollment_id TEXT NOT NULL PRIMARY KEY
        REFERENCES agent_registry_records(enrollment_id) ON DELETE CASCADE,
    signed_at_unix_ms INTEGER NOT NULL CHECK (signed_at_unix_ms >= 0)
) STRICT;
CREATE INDEX agent_registry_volume_scope
    ON agent_registry_records (tenant_id, storage_volume_id);
CREATE INDEX agent_registry_pvc_identity_scope
    ON agent_registry_records (edge_cluster_id, pvc_identity_digest);
CREATE UNIQUE INDEX agent_registry_active_volume_binding
    ON agent_registry_records (tenant_id, storage_volume_id, pvc_binding_role)
    WHERE pvc_binding_role IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_active_pvc_binding
    ON agent_registry_records (edge_cluster_id, pvc_identity_digest, pvc_binding_role)
    WHERE pvc_binding_role IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_token_request_identity
    ON agent_registry_records (tenant_id, token_request_id);
CREATE UNIQUE INDEX agent_registry_bootstrap_request_identity
    ON agent_registry_records (tenant_id, bootstrap_request_id)
    WHERE bootstrap_request_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_decision_request_identity
    ON agent_registry_records (tenant_id, decision_request_id)
    WHERE decision_request_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_installation_identity
    ON agent_registry_records (installation_id)
    WHERE installation_id IS NOT NULL;
CREATE UNIQUE INDEX agent_registry_public_key_identity
    ON agent_registry_records (public_key_fingerprint)
    WHERE public_key_fingerprint IS NOT NULL;
CREATE INDEX agent_registry_tenant_enrollment_keyset
    ON agent_registry_records (
        tenant_id, enrollment_created_at_unix_ms DESC, enrollment_id ASC
    );
CREATE INDEX agent_registry_tenant_status_keyset
    ON agent_registry_records (
        tenant_id, enrollment_state, registration_kind,
        enrollment_created_at_unix_ms DESC, enrollment_id ASC
    );
CREATE UNIQUE INDEX gateway_agent_cluster_identity
    ON agent_registry_records (agent_id, edge_cluster_id);
CREATE TABLE tenant_catalog_records (
    tenant_id TEXT NOT NULL PRIMARY KEY,
    display_name TEXT NOT NULL,
    description TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms)
) STRICT;
CREATE INDEX tenant_catalog_keyset
    ON tenant_catalog_records (created_at_unix_ms DESC, tenant_id ASC);
CREATE TABLE project_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, project_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id)
) STRICT;
CREATE INDEX project_catalog_keyset
    ON project_catalog_records (tenant_id, created_at_unix_ms DESC, project_id ASC);
CREATE TABLE artifact_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    description TEXT,
    initialization_mode TEXT NOT NULL CHECK (initialization_mode IN ('empty', 'derived')),
    source_project_id TEXT,
    source_artifact_id TEXT,
    source_commit_digest BLOB,
    head_commit_digest BLOB CHECK (
        head_commit_digest IS NULL OR length(head_commit_digest) = 32
    ),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (
        lifecycle_state IN ('active', 'pending_delete', 'deleting', 'restoring', 'deleted')
    ),
    lifecycle_generation TEXT NOT NULL DEFAULT '1' CHECK (
        lifecycle_generation <> '' AND lifecycle_generation NOT GLOB '*[^0-9]*'
    ),
    active_deletion_id TEXT,
    delete_requested_at_unix_ms INTEGER CHECK (delete_requested_at_unix_ms >= 0),
    purge_after_unix_ms INTEGER CHECK (purge_after_unix_ms >= 0),
    deleted_at_unix_ms INTEGER CHECK (deleted_at_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, artifact_id),
    UNIQUE (tenant_id, project_id, artifact_id),
    CHECK (
        (initialization_mode = 'empty' AND source_project_id IS NULL
            AND source_artifact_id IS NULL AND source_commit_digest IS NULL)
        OR
        (initialization_mode = 'derived' AND source_project_id IS NOT NULL
            AND source_artifact_id IS NOT NULL AND length(source_commit_digest) = 32)
    ),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id),
    FOREIGN KEY (tenant_id, source_project_id, source_artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id)
) STRICT;
CREATE INDEX artifact_catalog_keyset
    ON artifact_catalog_records (
        tenant_id, created_at_unix_ms DESC, project_id ASC, artifact_id ASC
    );
CREATE INDEX artifact_catalog_filter_keyset
    ON artifact_catalog_records (
        tenant_id, project_id, created_at_unix_ms DESC, artifact_id ASC
    );
CREATE TABLE storage_volume_catalog_records (
    tenant_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    edge_cluster_id TEXT NOT NULL,
    region TEXT NOT NULL,
    backend_type TEXT NOT NULL CHECK (backend_type IN ('pvc', 'nfs')),
    access_mode TEXT NOT NULL CHECK (
        access_mode IN ('read_write_once', 'read_write_many', 'read_only_many')
    ),
    allowed_delivery_modes TEXT NOT NULL DEFAULT '["fuse","copy"]',
    hardlink_policy TEXT NOT NULL DEFAULT 'disabled' CHECK (
        hardlink_policy IN ('disabled', 'sealed_acl', 'trusted_local')
    ),
    max_whole_file_bytes TEXT NOT NULL DEFAULT '18446744073709551615',
    copy_reserve_bytes TEXT NOT NULL DEFAULT '0',
    pvc_namespace TEXT,
    pvc_claim_name TEXT,
    nfs_server TEXT,
    nfs_export_path TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'degraded', 'unavailable')),
    enrollment_id TEXT,
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (
        lifecycle_state IN ('active', 'pending_delete', 'deleting', 'restoring', 'deleted')
    ),
    lifecycle_generation TEXT NOT NULL DEFAULT '1' CHECK (
        lifecycle_generation <> '' AND lifecycle_generation NOT GLOB '*[^0-9]*'
    ),
    active_deletion_id TEXT,
    delete_requested_at_unix_ms INTEGER CHECK (delete_requested_at_unix_ms >= 0),
    purge_after_unix_ms INTEGER CHECK (purge_after_unix_ms >= 0),
    deleted_at_unix_ms INTEGER CHECK (deleted_at_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, storage_volume_id),
    CHECK (
        (backend_type = 'pvc' AND pvc_namespace IS NOT NULL AND pvc_claim_name IS NOT NULL
            AND nfs_server IS NULL AND nfs_export_path IS NULL)
        OR
        (backend_type = 'nfs' AND pvc_namespace IS NULL AND pvc_claim_name IS NULL
            AND nfs_server IS NOT NULL AND nfs_export_path IS NOT NULL)
    )
) STRICT;
CREATE UNIQUE INDEX storage_volume_catalog_pvc_identity
    ON storage_volume_catalog_records (edge_cluster_id, pvc_namespace, pvc_claim_name)
    WHERE backend_type = 'pvc';
CREATE INDEX storage_volume_catalog_keyset
    ON storage_volume_catalog_records (
        tenant_id, created_at_unix_ms DESC, storage_volume_id ASC
    );
CREATE INDEX storage_volume_catalog_filter_keyset
    ON storage_volume_catalog_records (
        tenant_id, region, backend_type, created_at_unix_ms DESC, storage_volume_id ASC
    );
CREATE TABLE playground_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    playground_id TEXT NOT NULL,
    storage_volume_id TEXT NOT NULL,
    region TEXT NOT NULL,
    display_name TEXT NOT NULL,
    base_commit_digest BLOB CHECK (
        base_commit_digest IS NULL OR length(base_commit_digest) = 32
    ),
    head_commit_digest BLOB CHECK (
        head_commit_digest IS NULL OR length(head_commit_digest) = 32
    ),
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'abnormal')),
    relative_root TEXT NOT NULL CHECK (
        relative_root <> '' AND substr(relative_root, 1, 1) <> '/'
    ),
    resource_version TEXT NOT NULL DEFAULT '1' CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (
        lifecycle_state IN ('active', 'pending_delete', 'deleting', 'restoring', 'deleted')
    ),
    lifecycle_generation TEXT NOT NULL DEFAULT '1' CHECK (
        lifecycle_generation <> '' AND lifecycle_generation NOT GLOB '*[^0-9]*'
    ),
    active_deletion_id TEXT,
    delete_requested_at_unix_ms INTEGER CHECK (delete_requested_at_unix_ms >= 0),
    purge_after_unix_ms INTEGER CHECK (purge_after_unix_ms >= 0),
    deleted_at_unix_ms INTEGER CHECK (deleted_at_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, project_id, artifact_id, playground_id),
    FOREIGN KEY (tenant_id, project_id, artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id),
    FOREIGN KEY (tenant_id, storage_volume_id)
        REFERENCES storage_volume_catalog_records(tenant_id, storage_volume_id)
) STRICT;
CREATE INDEX playground_catalog_keyset
    ON playground_catalog_records (
        tenant_id, created_at_unix_ms DESC, project_id ASC, artifact_id ASC, playground_id ASC
    );
CREATE INDEX playground_catalog_filter_keyset
    ON playground_catalog_records (
        tenant_id, project_id, artifact_id, region, state,
        created_at_unix_ms DESC, playground_id ASC
    );
CREATE TABLE snapshot_catalog_records (
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    snapshot_request_id TEXT NOT NULL,
    commit_digest BLOB NOT NULL CHECK (length(commit_digest) = 32),
    state TEXT NOT NULL CHECK (state IN ('creating', 'ready', 'abnormal')),
    resource_version TEXT NOT NULL DEFAULT '1' CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    lifecycle_state TEXT NOT NULL DEFAULT 'active' CHECK (
        lifecycle_state IN ('active', 'pending_delete', 'deleting', 'restoring', 'deleted')
    ),
    lifecycle_generation TEXT NOT NULL DEFAULT '1' CHECK (
        lifecycle_generation <> '' AND lifecycle_generation NOT GLOB '*[^0-9]*'
    ),
    active_deletion_id TEXT,
    delete_requested_at_unix_ms INTEGER CHECK (delete_requested_at_unix_ms >= 0),
    purge_after_unix_ms INTEGER CHECK (purge_after_unix_ms >= 0),
    deleted_at_unix_ms INTEGER CHECK (deleted_at_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, snapshot_id),
    UNIQUE (tenant_id, snapshot_request_id),
    UNIQUE (tenant_id, project_id, artifact_id, commit_digest),
    FOREIGN KEY (tenant_id, project_id, artifact_id)
        REFERENCES artifact_catalog_records(tenant_id, project_id, artifact_id)
) STRICT;
CREATE INDEX snapshot_catalog_keyset
    ON snapshot_catalog_records (tenant_id, created_at_unix_ms DESC, snapshot_id ASC);
CREATE INDEX snapshot_catalog_filter_keyset
    ON snapshot_catalog_records (
        tenant_id, project_id, artifact_id, state,
        created_at_unix_ms DESC, snapshot_id ASC
    );
CREATE TABLE snapshot_delivery_records (
    tenant_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    create_request_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    commit_digest BLOB NOT NULL CHECK (length(commit_digest) = 32),
    storage_volume_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('fuse', 'copy', 'hardlink')),
    target_relative_root TEXT NOT NULL CHECK (
        target_relative_root <> '' AND substr(target_relative_root, 1, 1) <> '/'
    ),
    state TEXT NOT NULL CHECK (
        state IN ('requested', 'validating', 'materializing', 'ready', 'failed', 'deleting', 'deleted')
    ),
    source_index_digest BLOB NOT NULL CHECK (length(source_index_digest) = 32),
    delivery_generation TEXT NOT NULL CHECK (
        delivery_generation <> '' AND delivery_generation NOT GLOB '*[^0-9]*'
    ),
    file_count INTEGER NOT NULL CHECK (file_count >= 0),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    object_set_digest BLOB NOT NULL CHECK (length(object_set_digest) = 32),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    issue_code TEXT,
    issue_message TEXT,
    issue_retryable INTEGER NOT NULL CHECK (
        issue_retryable IN (0, 1) AND (issue_code IS NOT NULL OR issue_retryable = 0)
    ),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    PRIMARY KEY (tenant_id, delivery_id),
    UNIQUE (tenant_id, create_request_id),
    FOREIGN KEY (tenant_id, snapshot_id)
        REFERENCES snapshot_catalog_records(tenant_id, snapshot_id),
    FOREIGN KEY (tenant_id, storage_volume_id)
        REFERENCES storage_volume_catalog_records(tenant_id, storage_volume_id)
) STRICT;
CREATE INDEX snapshot_delivery_catalog_keyset
    ON snapshot_delivery_records (
        tenant_id, created_at_unix_ms DESC, delivery_id ASC
    );
CREATE TABLE snapshot_delivery_object_retention_roots (
    tenant_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    object_id BLOB NOT NULL CHECK (length(object_id) = 32),
    PRIMARY KEY (tenant_id, delivery_id, object_id),
    FOREIGN KEY (tenant_id, delivery_id)
        REFERENCES snapshot_delivery_records(tenant_id, delivery_id)
        ON DELETE CASCADE
) STRICT;
CREATE INDEX snapshot_delivery_retention_object_keyset
    ON snapshot_delivery_object_retention_roots (tenant_id, object_id, delivery_id);
CREATE TABLE snapshot_delivery_mutation_records (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('retry', 'delete')),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, delivery_id)
        REFERENCES snapshot_delivery_records(tenant_id, delivery_id)
) STRICT;
CREATE INDEX snapshot_delivery_catalog_snapshot
    ON snapshot_delivery_records (tenant_id, snapshot_id, state, mode);
CREATE TABLE s3_access_point_records (
    access_point_id TEXT NOT NULL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    commit_digest BLOB NOT NULL CHECK (length(commit_digest) = 32),
    bucket_name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('active', 'disabled')),
    policy_generation INTEGER NOT NULL CHECK (policy_generation > 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    UNIQUE (tenant_id, snapshot_id),
    FOREIGN KEY (tenant_id, snapshot_id)
        REFERENCES snapshot_catalog_records(tenant_id, snapshot_id)
) STRICT;
CREATE INDEX s3_access_point_tenant_keyset
    ON s3_access_point_records (
        tenant_id, created_at_unix_ms DESC, access_point_id ASC
    );
CREATE TABLE s3_credential_records (
    credential_id TEXT NOT NULL PRIMARY KEY,
    access_point_id TEXT NOT NULL,
    access_key_id TEXT NOT NULL UNIQUE,
    encrypted_secret BLOB NOT NULL CHECK (length(encrypted_secret) > 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'revoked', 'expired')),
    expires_at_unix_ms INTEGER NOT NULL CHECK (expires_at_unix_ms > 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    last_used_at_unix_ms INTEGER CHECK (last_used_at_unix_ms >= created_at_unix_ms),
    FOREIGN KEY (access_point_id)
        REFERENCES s3_access_point_records(access_point_id) ON DELETE CASCADE
) STRICT;
CREATE INDEX s3_credential_access_point_keyset
    ON s3_credential_records (access_point_id, created_at_unix_ms ASC, credential_id ASC);
CREATE TABLE s3_mutation_records (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (
        operation IN (
            'access_point_create', 'access_point_enable', 'access_point_disable',
            'credential_create', 'credential_revoke'
        )
    ),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id)
) STRICT;
CREATE TABLE gateway_pool_records (
    gateway_pool_id TEXT NOT NULL PRIMARY KEY,
    edge_cluster_id TEXT NOT NULL UNIQUE,
    agent_endpoint TEXT NOT NULL UNIQUE,
    s3_endpoint TEXT,
    state TEXT NOT NULL CHECK (state IN ('provisioning', 'ready', 'draining', 'disabled')),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    UNIQUE (gateway_pool_id, edge_cluster_id)
) STRICT;
CREATE UNIQUE INDEX gateway_pool_s3_endpoint_identity
    ON gateway_pool_records (s3_endpoint) WHERE s3_endpoint IS NOT NULL;
CREATE INDEX gateway_pool_state_keyset
    ON gateway_pool_records (state, gateway_pool_id);
CREATE TABLE gateway_replica_records (
    gateway_replica_id TEXT NOT NULL PRIMARY KEY,
    gateway_pool_id TEXT NOT NULL,
    edge_cluster_id TEXT NOT NULL,
    control_endpoint TEXT NOT NULL UNIQUE,
    peer_endpoint TEXT NOT NULL UNIQUE,
    bootstrap_endpoint TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'active', 'draining', 'revoked')),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    payload BLOB NOT NULL,
    UNIQUE (gateway_replica_id, gateway_pool_id, edge_cluster_id),
    FOREIGN KEY (gateway_pool_id, edge_cluster_id)
        REFERENCES gateway_pool_records (gateway_pool_id, edge_cluster_id)
) STRICT;
CREATE INDEX gateway_replica_pool_state_keyset
    ON gateway_replica_records (gateway_pool_id, state, gateway_replica_id);
CREATE TABLE gateway_replica_credentials (
    gateway_replica_id TEXT NOT NULL PRIMARY KEY
        REFERENCES gateway_replica_records (gateway_replica_id) ON DELETE CASCADE,
    activation_token_digest BLOB NOT NULL UNIQUE CHECK (length(activation_token_digest) = 32),
    state TEXT NOT NULL CHECK (
        state IN (
            'pending_activation', 'pending_certificate_delivery', 'active', 'expired', 'revoked'
        )
    ),
    certificate_generation TEXT CHECK (
        certificate_generation IS NULL OR (
            certificate_generation <> '' AND certificate_generation NOT GLOB '*[^0-9]*'
        )
    ),
    payload BLOB NOT NULL
) STRICT;
CREATE INDEX gateway_replica_credential_state
    ON gateway_replica_credentials (state, gateway_replica_id);
CREATE TABLE agent_route_leases (
    agent_id TEXT NOT NULL PRIMARY KEY,
    edge_cluster_id TEXT NOT NULL,
    gateway_pool_id TEXT NOT NULL,
    gateway_replica_id TEXT NOT NULL,
    connection_id TEXT NOT NULL UNIQUE,
    session_generation TEXT NOT NULL CHECK (
        session_generation <> '' AND session_generation NOT GLOB '*[^0-9]*'
    ),
    route_generation TEXT NOT NULL CHECK (
        route_generation <> '' AND route_generation NOT GLOB '*[^0-9]*'
    ),
    acquire_request_id TEXT NOT NULL UNIQUE,
    last_renew_request_id TEXT UNIQUE,
    release_request_id TEXT UNIQUE,
    lease_expires_at_unix_ms INTEGER NOT NULL CHECK (lease_expires_at_unix_ms >= 0),
    released_at_unix_ms INTEGER CHECK (released_at_unix_ms IS NULL OR released_at_unix_ms >= 0),
    payload BLOB NOT NULL,
    FOREIGN KEY (agent_id, edge_cluster_id)
        REFERENCES agent_registry_records (agent_id, edge_cluster_id),
    FOREIGN KEY (gateway_replica_id, gateway_pool_id, edge_cluster_id)
        REFERENCES gateway_replica_records (
            gateway_replica_id, gateway_pool_id, edge_cluster_id
        )
) STRICT;
CREATE INDEX agent_route_owner_keyset
    ON agent_route_leases (gateway_pool_id, gateway_replica_id, agent_id);
CREATE INDEX agent_route_expiry_keyset
    ON agent_route_leases (gateway_pool_id, lease_expires_at_unix_ms, agent_id);
CREATE TABLE deletion_impact_records (
    tenant_id TEXT NOT NULL,
    impact_digest BLOB NOT NULL CHECK (length(impact_digest) = 32),
    expires_at_unix_ms INTEGER NOT NULL CHECK (expires_at_unix_ms >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, impact_digest),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id)
) STRICT;
CREATE INDEX deletion_impact_expiry_keyset
    ON deletion_impact_records (expires_at_unix_ms, tenant_id, impact_digest);
CREATE TABLE deletion_operation_records (
    tenant_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'requested', 'quiescing', 'quarantining', 'recoverable', 'restoring',
            'purging', 'finalizing', 'completed', 'blocked', 'failed'
        )
    ),
    resource_version TEXT NOT NULL CHECK (
        resource_version <> '' AND resource_version NOT GLOB '*[^0-9]*'
    ),
    request_id TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    impact_digest BLOB NOT NULL CHECK (length(impact_digest) = 32),
    purge_after_unix_ms INTEGER NOT NULL CHECK (purge_after_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    updated_at_unix_ms INTEGER NOT NULL CHECK (updated_at_unix_ms >= created_at_unix_ms),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, deletion_id),
    UNIQUE (tenant_id, request_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant_catalog_records(tenant_id)
) STRICT;
CREATE INDEX deletion_operation_state_keyset
    ON deletion_operation_records (
        tenant_id, state, created_at_unix_ms DESC, deletion_id ASC
    );
CREATE TABLE deletion_mutation_records (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (
        kind IN ('create', 'restore', 'retry', 'retention_hold_create', 'retention_hold_release')
    ),
    deletion_id TEXT NOT NULL,
    retention_hold_id TEXT,
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, deletion_id)
        REFERENCES deletion_operation_records(tenant_id, deletion_id)
) STRICT;
CREATE TABLE retention_hold_records (
    tenant_id TEXT NOT NULL,
    retention_hold_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'released')),
    expires_at_unix_ms INTEGER CHECK (expires_at_unix_ms >= 0),
    created_at_unix_ms INTEGER NOT NULL CHECK (created_at_unix_ms >= 0),
    released_at_unix_ms INTEGER CHECK (released_at_unix_ms >= created_at_unix_ms),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, retention_hold_id),
    FOREIGN KEY (tenant_id, deletion_id)
        REFERENCES deletion_operation_records(tenant_id, deletion_id)
) STRICT;
CREATE INDEX retention_hold_deletion_keyset
    ON retention_hold_records (tenant_id, deletion_id, state, created_at_unix_ms ASC);
CREATE TABLE lifecycle_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    occurred_at_unix_ms INTEGER NOT NULL CHECK (occurred_at_unix_ms >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id, deletion_id)
        REFERENCES deletion_operation_records(tenant_id, deletion_id)
) STRICT;
CREATE TABLE deletion_proofs (
    tenant_id TEXT NOT NULL,
    proof_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    completed_at_unix_ms INTEGER NOT NULL CHECK (completed_at_unix_ms >= 0),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, proof_id),
    FOREIGN KEY (tenant_id, deletion_id)
        REFERENCES deletion_operation_records(tenant_id, deletion_id)
) STRICT;
CREATE TABLE lifecycle_assignment_outbox (
    tenant_id TEXT NOT NULL,
    assignment_id TEXT NOT NULL,
    deletion_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0, 1)),
    retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0, 1)),
    payload BLOB NOT NULL,
    PRIMARY KEY (tenant_id, assignment_id),
    FOREIGN KEY (tenant_id, deletion_id)
        REFERENCES deletion_operation_records(tenant_id, deletion_id),
    CHECK (retired = 0 OR published = 1)
) STRICT;
CREATE INDEX lifecycle_assignment_delivery
    ON lifecycle_assignment_outbox (
        agent_id, published, retired, tenant_id, assignment_id
    );
"#;
