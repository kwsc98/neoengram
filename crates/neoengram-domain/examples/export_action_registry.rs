use std::io::{self, Write as _};

use neoengram_domain::protocol::{
    central_action_registry, AgentActionTransport, GatewayActionListener, AGENT_ACTION_REGISTRY,
    GATEWAY_ACTION_REGISTRY, INTERNAL_ACTION_REGISTRY, PUBLIC_ACTION_REGISTRY,
};
use serde::Serialize;

#[derive(Serialize)]
struct ActionRegistryExport<'a> {
    schema_version: u32,
    central_routes: Vec<CentralRoute<'a>>,
    public_openapi: Vec<PublicRoute<'a>>,
    agent_actions: Vec<AgentRoute<'a>>,
    gateway_fixed_actions: Vec<GatewayRoute<'a>>,
}

#[derive(Serialize)]
struct CentralRoute<'a> {
    method: &'a str,
    path: &'a str,
    operation_id: &'a str,
    visibility: &'static str,
}

#[derive(Serialize)]
struct PublicRoute<'a> {
    method: &'a str,
    path: &'a str,
    operation_id: &'a str,
    routed_by_central: bool,
}

#[derive(Serialize)]
struct AgentRoute<'a> {
    method: &'static str,
    path: &'a str,
    operation_id: &'a str,
    transport: &'static str,
    requires_agent_proof: bool,
}

#[derive(Serialize)]
struct GatewayRoute<'a> {
    method: &'a str,
    path: &'a str,
    listener: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut central_routes = central_action_registry()
        .map(|descriptor| CentralRoute {
            method: descriptor.method,
            path: descriptor.path,
            operation_id: descriptor.operation_id,
            visibility: if INTERNAL_ACTION_REGISTRY
                .iter()
                .any(|internal| internal.path == descriptor.path)
            {
                "internal"
            } else {
                "public"
            },
        })
        .collect::<Vec<_>>();
    central_routes.sort_unstable_by_key(|route| (route.path, route.method));

    let public_openapi = PUBLIC_ACTION_REGISTRY
        .iter()
        .map(|descriptor| PublicRoute {
            method: descriptor.method,
            path: descriptor.path,
            operation_id: descriptor.operation_id,
            routed_by_central: descriptor.routed_by_central,
        })
        .collect();
    let agent_actions = AGENT_ACTION_REGISTRY
        .iter()
        .map(|descriptor| AgentRoute {
            method: descriptor.method,
            path: descriptor.path,
            operation_id: descriptor.operation_id,
            transport: match descriptor.transport {
                AgentActionTransport::HttpPost => "http-post",
                AgentActionTransport::Http2Ndjson => "http2-ndjson",
            },
            requires_agent_proof: descriptor.requires_agent_proof,
        })
        .collect();
    let gateway_fixed_actions = GATEWAY_ACTION_REGISTRY
        .iter()
        .map(|descriptor| GatewayRoute {
            method: descriptor.method,
            path: descriptor.path,
            listener: match descriptor.listener {
                GatewayActionListener::All => "all",
                GatewayActionListener::Agent => "agent",
                GatewayActionListener::Control => "control",
                GatewayActionListener::Peer => "peer",
            },
        })
        .collect();

    let export = ActionRegistryExport {
        schema_version: 1,
        central_routes,
        public_openapi,
        agent_actions,
        gateway_fixed_actions,
    };
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, &export)?;
    output.write_all(b"\n")?;
    Ok(())
}
