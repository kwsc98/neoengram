#![cfg(feature = "authority-sqlite")]

use std::path::Path;

use neoengram_central::{open_sqlite_authority, CentralErrorCode, SqliteAuthorityConfig};
use sqlx::{sqlite::SqliteConnectOptions, Connection, SqliteConnection};
use tempfile::TempDir;

const SQLITE_APPLICATION_ID: i64 = 0x4e45_4155;
const SQLITE_SCHEMA_VERSION: i64 = 21;

#[tokio::test]
async fn fresh_authority_advertises_v2_identity_and_materialization_tables() {
    let directory = TempDir::new().unwrap();
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .unwrap();
    authority.close().await;

    let mut connection = connect(directory.path()).await;
    let application_id: i64 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    let user_version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(application_id, SQLITE_APPLICATION_ID);
    assert_eq!(user_version, SQLITE_SCHEMA_VERSION);

    for table in [
        "object_placements",
        "managed_object_placement_evidence",
        "volume_commit_coverages",
        "materializations",
        "materialization_batches",
        "materialization_objects",
        "object_read_leases",
        "staging_leases",
        "materialization_receipts",
        "operation_tasks",
        "task_request_identities",
        "task_attempts",
        "task_events",
        "task_stages",
        "task_stage_history",
        "task_stage_dependencies",
        "task_resource_links",
        "task_relations",
    ] {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(count, 1, "missing v2 authority table {table}");
    }
    let obsolete_v2_name: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'object_placements_v2'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        obsolete_v2_name, 0,
        "versioned v2 placement table must not be installed"
    );

    for legacy_table in [
        "legacy_commit_placement_sets",
        "legacy_placement_objects",
        "legacy_replications",
        "legacy_replication_artifacts",
        "legacy_replication_retry_mutations",
        "legacy_replication_objects",
    ] {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ?",
        )
        .bind(legacy_table)
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(
            count, 1,
            "legacy {legacy_table} table is retained only for compiled v1 paths and is not v2 authority"
        );
    }
    for obsolete_name in [
        "commit_placement_sets",
        "replications",
        "replication_objects",
        "commit_placement_sets_lookup",
        "replications_active_target",
        "placement_objects_lookup",
    ] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name = ?")
            .bind(obsolete_name)
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "unprefixed v1 table {obsolete_name} must not be installed"
        );
    }
    connection.close().await.unwrap();
}

#[tokio::test]
async fn authority_rejects_unexpected_schema_objects_without_implicit_migration() {
    let directory = TempDir::new().unwrap();
    initialize_and_close(directory.path()).await;
    execute_raw(
        directory.path(),
        "CREATE TABLE legacy_replication_records (id TEXT NOT NULL) STRICT;",
    )
    .await;

    let error = open_sqlite_authority(SqliteAuthorityConfig::new(directory.path()))
        .await
        .expect_err("unknown schema objects must be rejected");
    assert_eq!(error.code(), CentralErrorCode::StorageFailure);
    assert!(error.to_string().contains("table or index set"));
}

#[tokio::test]
async fn v2_materialization_keys_are_namespace_scoped_and_active_index_is_partial() {
    let directory = TempDir::new().unwrap();
    initialize_and_close(directory.path()).await;
    let mut connection = connect(directory.path()).await;

    let materializations_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'materializations'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert!(materializations_sql.contains("UNIQUE (tenant_id, object_namespace_id, request_id)"));
    assert!(materializations_sql
        .contains("PRIMARY KEY (tenant_id, object_namespace_id, materialization_id)"));
    assert!(!materializations_sql.contains("UNIQUE (materialization_id)"));

    let objects_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = 'materialization_objects'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert!(objects_sql
        .contains("UNIQUE (tenant_id, materialization_id, object_namespace_id, staging_key)"));

    for table in ["object_read_leases", "staging_leases"] {
        let sql: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?")
                .bind(table)
                .fetch_one(&mut connection)
                .await
                .unwrap();
        assert!(
            sql.contains("PRIMARY KEY (tenant_id, object_namespace_id, lease_id)"),
            "{table} lease identity must include namespace"
        );
        assert!(
            !sql.contains("UNIQUE (tenant_id, lease_id)"),
            "{table} must not install a tenant-only lease uniqueness key"
        );
    }

    let active_index_sql: String = sqlx::query_scalar(
        "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = 'materializations_active_target'",
    )
    .fetch_one(&mut connection)
    .await
    .unwrap();
    assert!(active_index_sql.contains("WHERE state IN"));
    assert!(active_index_sql.contains("'stalled'"));
    connection.close().await.unwrap();
}

#[tokio::test]
async fn terminal_materialization_does_not_hold_the_active_target_key() {
    let directory = TempDir::new().unwrap();
    initialize_and_close(directory.path()).await;
    let mut connection = connect(directory.path()).await;

    insert_materialization_row(
        &mut connection,
        "materialization-complete",
        "complete",
        "request-complete",
    )
    .await;
    insert_materialization_row(
        &mut connection,
        "materialization-active",
        "queued",
        "request-active",
    )
    .await;
    let duplicate = insert_materialization_row_result(
        &mut connection,
        "materialization-second-active",
        "planning",
        "request-second-active",
    )
    .await;
    assert!(
        duplicate.is_err(),
        "two active Jobs must conflict on one target key"
    );
    connection.close().await.unwrap();
}

async fn insert_materialization_row(
    connection: &mut SqliteConnection,
    materialization_id: &str,
    state: &str,
    request_id: &str,
) {
    insert_materialization_row_result(connection, materialization_id, state, request_id)
        .await
        .unwrap();
}

async fn insert_materialization_row_result(
    connection: &mut SqliteConnection,
    materialization_id: &str,
    state: &str,
    request_id: &str,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO materializations (tenant_id, materialization_id, object_namespace_id, \
         artifact_id, commit_id, target_storage_volume_id, coverage_goal, coverage_goal_value, \
         object_set_digest, plan_revision, object_count, total_bytes, verified_object_count, \
         verified_bytes, missing_object_count, missing_bytes, source_count, deadline_unix_ms, \
         state, request_id, payload, created_at_unix_ms, updated_at_unix_ms) \
         VALUES ('tenant', ?, 'namespace', 'namespace', zeroblob(32), 'volume', 'complete', 0, \
         zeroblob(32), 1, 0, 0, 0, 0, 0, 0, 0, 1, ?, ?, x'00', 0, 0)",
    )
    .bind(materialization_id)
    .bind(state)
    .bind(request_id)
    .execute(connection)
    .await
}

async fn initialize_and_close(path: &Path) {
    let authority = open_sqlite_authority(SqliteAuthorityConfig::new(path))
        .await
        .unwrap();
    authority.close().await;
}

async fn connect(root: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(root.join("authority.sqlite3")),
    )
    .await
    .unwrap()
}

async fn execute_raw(root: &Path, sql: &str) {
    let mut connection = connect(root).await;
    sqlx::raw_sql(sql).execute(&mut connection).await.unwrap();
    connection.close().await.unwrap();
}
