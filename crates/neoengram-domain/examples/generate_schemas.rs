use std::{fs, path::PathBuf};

use neoengram_domain::protocol::{
    action_schema, control_schema, enrollment_schema, gateway_schema, metadata_schema,
    snapshot_delivery_schema,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("schemas/current");
    fs::create_dir_all(&output)?;

    write_schema(
        output.join("control-envelope.schema.json"),
        &control_schema(),
    )?;
    write_schema(output.join("action-envelope.schema.json"), &action_schema())?;
    write_schema(
        output.join("agent-enrollment.schema.json"),
        &enrollment_schema(),
    )?;
    write_schema(
        output.join("metadata-batch.schema.json"),
        &metadata_schema(),
    )?;
    write_schema(
        output.join("gateway-control.schema.json"),
        &gateway_schema(),
    )?;
    write_schema(
        output.join("snapshot-delivery.schema.json"),
        &snapshot_delivery_schema(),
    )?;
    Ok(())
}

fn write_schema(
    path: PathBuf,
    schema: &schemars::Schema,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = serde_json::to_vec_pretty(schema)?;
    bytes.push(b'\n');
    fs::write(path, bytes)?;
    Ok(())
}
