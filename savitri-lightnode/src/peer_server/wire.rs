use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodePresence {
    pub peer_id: String,
    pub network_id: String,
    #[serde(
        default,
        alias = "listen_addrs",
        alias = "multiaddrs",
        alias = "addresses"
    )]
    pub listen_addresses: Vec<String>,
    #[serde(default)]
    pub supported_protocols: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnregisterRequest {
    pub peer_id: String,
    pub network_id: String,
}

#[derive(Debug, Clone, Deserialize, Default, PartialEq, Eq)]
pub struct PeerRecord {
    pub peer_id: String,
    #[serde(default)]
    pub network_id: Option<String>,
    #[serde(
        default,
        alias = "listen_addrs",
        alias = "multiaddrs",
        alias = "addresses"
    )]
    pub listen_addresses: Vec<String>,
    #[serde(default)]
    pub supported_protocols: Vec<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub agent_version: Option<String>,
    #[serde(default)]
    pub build_version: Option<String>,
    #[serde(default)]
    pub rpc_endpoint: Option<String>,
    #[serde(default)]
    pub last_seen: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum GetPeersResponse {
    Wrapped { peers: Vec<PeerRecord> },
    DataWrapped { data: Vec<PeerRecord> },
    List(Vec<PeerRecord>),
}

impl GetPeersResponse {
    pub fn into_peers(self) -> Vec<PeerRecord> {
        match self {
            Self::Wrapped { peers } => peers,
            Self::DataWrapped { data } => data,
            Self::List(peers) => peers,
        }
    }
}
