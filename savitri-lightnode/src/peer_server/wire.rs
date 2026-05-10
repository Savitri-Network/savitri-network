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

#[cfg(test)]
mod tests {
    use super::{GetPeersResponse, PeerRecord};

    #[test]
    fn parses_wrapped_peers_response() {
        let parsed: GetPeersResponse = serde_json::from_str(
            r#"{
                "peers": [
                    {
                        "peer_id": "12D3KooWRapped",
                        "network_id": "testnet",
                        "listen_addrs": ["/ip4/198.51.100.1/tcp/4001"],
                        "roles": ["lightnode"]
                    }
                ]
            }"#,
        )
        .expect("response should parse");

        let peers = parsed.into_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0].listen_addresses,
            vec!["/ip4/198.51.100.1/tcp/4001"]
        );
    }

    #[test]
    fn parses_list_peers_response() {
        let parsed: GetPeersResponse = serde_json::from_str(
            r#"[
                {
                    "peer_id": "12D3KooWList",
                    "multiaddrs": ["/ip4/203.0.113.7/tcp/4002"],
                    "supported_protocols": ["/savitri/1.0.0"]
                }
            ]"#,
        )
        .expect("response should parse");

        let peers = parsed.into_peers();
        assert_eq!(
            peers,
            vec![PeerRecord {
                peer_id: "12D3KooWList".to_string(),
                network_id: None,
                listen_addresses: vec!["/ip4/203.0.113.7/tcp/4002".to_string()],
                supported_protocols: vec!["/savitri/1.0.0".to_string()],
                roles: Vec::new(),
                agent_version: None,
                build_version: None,
                rpc_endpoint: None,
                last_seen: None,
            }]
        );
    }
}
