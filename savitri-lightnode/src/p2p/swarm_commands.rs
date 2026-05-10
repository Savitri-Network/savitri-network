//! Swarm Command Queue Pattern - Tipi condivisi per la comunicazione tra task.
//!
//! - `SwarmCommand`: comandi inviati dai task worker allo swarm task
//! - `NetworkEvent`: eventi emessi dallo swarm task verso i task worker
//!
//! Pattern used da Lighthouse (Ethereum), Substrate/Polkadot, e dalla
//! documentazione ufficiale libp2p per isolare lo swarm in un task dedicato.

use libp2p::{gossipsub::IdentTopic, Multiaddr, PeerId};

use crate::p2p::aux_protocol::AuxMessage;
use crate::p2p::consensus_protocol::ConsensusMessage;

/// Comandi inviati allo swarm task tramite canale mpsc.
///
/// Lo swarm task e' l'unico proprietario di `Swarm<MyBehaviour>`.
/// attraverso un canale `mpsc::Sender<SwarmCommand>`.
#[derive(Debug)]
pub enum SwarmCommand {
    /// Pubblica un messaggio gossipsub su un topic.
    Publish { topic: IdentTopic, payload: Vec<u8> },

    /// Pubblica piu' messaggi gossipsub in sequenza (atomicamente dal punto di vista of the chiamante).
    PublishBatch {
        messages: Vec<(IdentTopic, Vec<u8>)>,
    },

    /// Dial un peer con gli indirizzi specificati.
    Dial {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },

    /// Sottoscrive lo swarm a un topic gossipsub.
    Subscribe { topic: IdentTopic },

    /// Rimuove la sottoscrizione a un topic gossipsub.
    Unsubscribe { topic: IdentTopic },

    /// Adds un peer esplicito alla mesh gossipsub (prioritario).
    AddExplicitPeer { peer_id: PeerId },

    /// Rimuove un peer esplicito dalla mesh gossipsub.
    RemoveExplicitPeer { peer_id: PeerId },

    /// Inserisce un record in the DHT Kademlia.
    KadPutRecord { key: String, value: Vec<u8> },

    /// Richiede un record dalla DHT Kademlia.
    KadGetRecord { key: String },

    /// Send a consensus message directly to a peer via request-response protocol.
    SendConsensusRequest {
        peer_id: PeerId,
        message: ConsensusMessage,
    },

    /// Send an auxiliary message directly to a peer via request-response protocol.
    /// Used for heartbeat, PoU, peer_discovery, peer_registry (moved off gossipsub
    /// to prevent Send Queue saturation).
    SendAuxRequest {
        peer_id: PeerId,
        message: AuxMessage,
    },

    /// Send a block sync request directly to a peer via request-response protocol.
    SendBlockSyncRequest {
        peer_id: PeerId,
        request: crate::p2p::block_sync::BlockSyncRequest,
    },

    Shutdown,
}

/// Eventi emessi dallo swarm task verso gli altri task tramite broadcast channel.
///
/// I task worker (maintenance, publish aggregator) si sottoscrivono a questi
/// eventi per mantenere il proprio stato aggiornato without accesso diretto allo swarm.
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// Un peer si e' connesso.
    PeerConnected {
        peer_id: PeerId,
        is_masternode: bool,
    },

    /// Un peer si e' disconnesso.
    PeerDisconnected { peer_id: PeerId },

    /// Errore di connessione in uscita.
    OutgoingConnectionError {
        peer_id: Option<PeerId>,
        error: String,
    },

    /// Messaggio gossipsub ricevuto.
    GossipMessage {
        topic_hash: libp2p::gossipsub::TopicHash,
        data: Vec<u8>,
        source: Option<PeerId>,
    },

    /// Un peer si e' sottoscritto a un topic gossipsub.
    GossipSubscribed {
        peer_id: PeerId,
        topic: libp2p::gossipsub::TopicHash,
    },

    GroupMembersUpdated {
        group_id: String,
        members: std::collections::HashSet<PeerId>,
        addresses: std::collections::HashMap<PeerId, Multiaddr>,
        mesh_established: bool,
    },

    /// Nuovo indirizzo di ascolto.
    NewListenAddr { address: Multiaddr },

    /// Block sync response received from a peer.
    BlockSyncResponse {
        peer_id: PeerId,
        response: crate::p2p::block_sync::BlockSyncResponse,
    },
}
