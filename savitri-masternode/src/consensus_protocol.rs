//! Direct P2P consensus protocol using libp2p request-response.
//!
//! Identical to the lightnode consensus_protocol module. Moves consensus messages
//! (votes, certificates) from gossipsub broadcast to direct peer-to-peer streams.

use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response, StreamProtocol};
use serde::{Deserialize, Serialize};
use std::io;

/// Protocol identifier for consensus direct messaging.
pub const CONSENSUS_PROTOCOL: &str = "/savitri/consensus/1.0.0";

const MAX_REQUEST_SIZE: usize = 1_048_576;
const MAX_RESPONSE_SIZE: usize = 65_536;

/// Consensus message types sent directly between masternode peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    /// MasternodeVote (block vote between MNs)
    Vote(Vec<u8>),
    /// BlockAcceptanceCertificate (owner MN -> other MNs)
    BlockAcceptance(Vec<u8>),
    /// Group-related masternode messages (proposals, votes, sync)
    MasternodeMessage(Vec<u8>),
}

/// Simple ACK response for consensus messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusAck {
    pub ok: bool,
}

/// Length-prefixed bincode codec for consensus request-response.
#[derive(Debug, Clone, Default)]
pub struct ConsensusCodec;

#[async_trait::async_trait]
impl request_response::Codec for ConsensusCodec {
    type Protocol = StreamProtocol;
    type Request = ConsensusMessage;
    type Response = ConsensusAck;

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
                format!("consensus request too large: {} bytes", len),
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
                format!("consensus response too large: {} bytes", len),
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

/// Create a configured request-response behaviour for consensus.
pub fn build_consensus_behaviour() -> request_response::Behaviour<ConsensusCodec> {
    let protocol = StreamProtocol::new(CONSENSUS_PROTOCOL);
    request_response::Behaviour::new(
        [(protocol, request_response::ProtocolSupport::Full)],
        request_response::Config::default(),
    )
}
