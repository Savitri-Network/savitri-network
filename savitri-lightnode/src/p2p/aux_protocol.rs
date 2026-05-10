//! Direct P2P auxiliary protocol using libp2p request-response.
//!
//! Moves high-frequency auxiliary messages (heartbeat, PoU, peer discovery,
//! peer registry) from gossipsub broadcast to direct peer-to-peer TCP streams.
//! This prevents gossipsub Send Queue saturation caused by forwarding these
//! messages to all mesh peers.

use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response, StreamProtocol};
use serde::{Deserialize, Serialize};
use std::io;

/// Protocol identifier for auxiliary direct messaging.
pub const AUX_PROTOCOL: &str = "/savitri/aux/1.0.0";

const MAX_REQUEST_SIZE: usize = 524_288; // 512KB
const MAX_RESPONSE_SIZE: usize = 65_536;

/// Auxiliary message types sent directly between peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AuxMessage {
    /// Heartbeat ping/pong between nodes
    Heartbeat(Vec<u8>),
    /// Proof-of-Uptime broadcast
    PoU(Vec<u8>),
    /// Peer discovery request (asking for known peers)
    PeerDiscoveryRequest(Vec<u8>),
    /// Peer discovery response (list of known peers)
    PeerDiscoveryResponse(Vec<u8>),
    /// Peer registry update (node info broadcast)
    PeerRegistry(Vec<u8>),
    /// Pull-based block synchronization request/response payloads (bincode-encoded).
    BlockSync(Vec<u8>),
    /// Cross-group TX forwarded directly to the elected proposer of the target
    /// group (Tier 4 Fase 2 step 5). Payload is the raw signed transaction
    /// bytes — same format as the gossipsub `Transaction` variant. The
    /// returns `AuxAck { ok: true, payload: None }` on successful submit
    /// to the local mempool, `ok: false` otherwise (sender will fall back
    /// to gossipsub).
    TxForward(Vec<u8>),
}

/// Simple ACK response for auxiliary messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuxAck {
    pub ok: bool,
    /// Optional response payload (e.g. peer discovery response)
    pub payload: Option<Vec<u8>>,
}

/// Length-prefixed bincode codec for auxiliary request-response.
#[derive(Debug, Clone, Default)]
pub struct AuxCodec;

#[async_trait::async_trait]
impl request_response::Codec for AuxCodec {
    type Protocol = StreamProtocol;
    type Request = AuxMessage;
    type Response = AuxAck;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("aux request too large: {} bytes", len),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_RESPONSE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("aux response too large: {} bytes", len),
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let buf =
            bincode::serialize(&req).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = (buf.len() as u32).to_le_bytes();
        io.write_all(&len).await?;
        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let buf =
            bincode::serialize(&res).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let len = (buf.len() as u32).to_le_bytes();
        io.write_all(&len).await?;
        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }
}

/// Create a configured request-response behaviour for auxiliary messages.
pub fn build_aux_behaviour() -> request_response::Behaviour<AuxCodec> {
    let protocol = StreamProtocol::new(AUX_PROTOCOL);
    request_response::Behaviour::new(
        [(protocol, request_response::ProtocolSupport::Full)],
        request_response::Config::default(),
    )
}
