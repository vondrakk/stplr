// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 The Von Drakk Corporation
//! Node configuration: the roles a process runs, plus ports/storage/replication.
//!
//! A Stitch node is a single binary that can run any combination of roles (the Elasticsearch
//! node-roles model). Roles can be changed later by editing this config or via `stitch role`,
//! and a running node applies the change on SIGHUP. The data-bearing role is `shard`; the
//! others (coordinator, ingest, api) scale on their own axes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// A capability a node process can run. Compose freely (e.g. `shard,ingest`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Owns and serves a partition of the data. The horizontal-scale unit (rebalances on change).
    Shard,
    /// Routes queries/ingest to shards and drives rebalancing. The migration driver is singleton.
    Coordinator,
    /// Parses/extracts documents into values. Stateless; scales on write throughput.
    Ingest,
    /// MCP + HTTP protocol endpoint. Stateless; scale for HA (e.g. 3 replicas behind an LB).
    Api,
}

impl Role {
    pub const ALL: [Role; 4] = [Role::Shard, Role::Coordinator, Role::Ingest, Role::Api];

    pub fn as_str(self) -> &'static str {
        match self {
            Role::Shard => "shard",
            Role::Coordinator => "coordinator",
            Role::Ingest => "ingest",
            Role::Api => "api",
        }
    }

    pub fn parse(s: &str) -> Result<Role> {
        match s.trim().to_ascii_lowercase().as_str() {
            "shard" => Ok(Role::Shard),
            "coordinator" | "coord" => Ok(Role::Coordinator),
            "ingest" => Ok(Role::Ingest),
            "api" | "mcp" => Ok(Role::Api),
            other => anyhow::bail!("unknown role '{other}' (expected: shard, coordinator, ingest, api, all)"),
        }
    }

    /// Parse a comma/space list of role names, expanding "all".
    pub fn parse_list(items: &[String]) -> Result<BTreeSet<Role>> {
        let mut set = BTreeSet::new();
        for item in items {
            for part in item.split([',', ' ']) {
                let p = part.trim();
                if p.is_empty() {
                    continue;
                }
                if p.eq_ignore_ascii_case("all") {
                    set.extend(Role::ALL);
                } else {
                    set.insert(Role::parse(p)?);
                }
            }
        }
        Ok(set)
    }
}

/// Per-shard storage backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoreKind {
    /// Non-persistent; rebuilt from re-ingest. Dev/eval only.
    Memory,
    /// Embedded memory-mapped store on the node's disk. The PVC is the database.
    Lmdb,
    /// Co-located Redis with native set-logic pushdown.
    Redis,
}

impl Default for StoreKind {
    fn default() -> Self {
        StoreKind::Lmdb
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub node_id: String,
    /// Roles this node runs. At least one required.
    pub roles: BTreeSet<Role>,
    #[serde(default = "default_host")]
    pub bind_host: String,
    #[serde(default = "default_shard_port")]
    pub shard_port: u16,
    #[serde(default = "default_coordinator_port")]
    pub coordinator_port: u16,
    #[serde(default = "default_api_port")]
    pub api_port: u16,
    #[serde(default)]
    pub store: StoreKind,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Copies of each data hash across shards (R). Default 2 — every hash is replicated, so a
    /// node can be lost (or drained) without data loss; drain/auto-heal restore R.
    #[serde(default = "default_replication")]
    pub replication: u32,
    /// Coordinator: explicit shard endpoints ("id=url") or a discovery domain (wired later).
    #[serde(default)]
    pub shards: Vec<String>,
}

fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_shard_port() -> u16 {
    8100
}
fn default_coordinator_port() -> u16 {
    8090
}
fn default_api_port() -> u16 {
    8088
}
fn default_data_dir() -> String {
    "/var/lib/stitch".into()
}
fn default_replication() -> u32 {
    2
}

impl NodeConfig {
    pub fn load(path: &Path) -> Result<NodeConfig> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let cfg: NodeConfig = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        Ok(cfg)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).ok();
            }
        }
        let text = toml::to_string_pretty(self).context("serializing config")?;
        std::fs::write(path, text).with_context(|| format!("writing config {}", path.display()))?;
        Ok(())
    }

    pub fn roles_str(&self) -> String {
        self.roles.iter().map(|r| r.as_str()).collect::<Vec<_>>().join(",")
    }
}

pub fn default_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("STITCH_CONFIG") {
        return PathBuf::from(p);
    }
    PathBuf::from("stitch.toml")
}
