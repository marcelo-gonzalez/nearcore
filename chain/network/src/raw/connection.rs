use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerChainInfoV2, PeerIdOrHash,
    PeerMessage, Ping, Pong, RawRoutedMessage, RoutedMessageBody, RoutingTableUpdate,
};
use crate::routing::route_back_cache::RouteBackCache;
use crate::types::StateResponseInfo;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::time::{Clock, Duration, Instant, Utc};
use near_primitives::types::{BlockHeight, ShardId};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Represents a connection to a peer, and provides only minimal functionality.
/// Almost none of the usual NEAR network logic is implemented, and the caller
/// will receive messages via the recv() function. Currently the only message
/// you can send is a Ping message, but in the future we can add more stuff there
/// (e.g. sending blocks/chunk parts/etc)
pub struct Connection {
    my_peer_id: PeerId,
    peer_id: PeerId,
    secret_key: SecretKey,
    stream: TcpStream,
    buf: BytesMut,
    recv_timeout: Duration,
    // this is used to keep track of routed messages we've sent so that when we get a reply
    // that references one of our previously sent messages, we can determine that the message is for us
    route_cache: RouteBackCache,
    clock: Clock,
}

// The types of messages it's possible to route to a target PeerId via the connected peer as a first hop
// These can be sent with Connection::send_routed_message(), and received in Message::Routed() from
// Connection::recv()
#[derive(Clone, Debug)]
pub enum RoutedMessage {
    Ping { nonce: u64 },
    Pong { nonce: u64, source: PeerId },
    StateRequestPart(ShardId, CryptoHash, u64),
    VersionedStateResponse(StateResponseInfo),
}

// The types of messages it's possible to send directly to the connected peer.
// These can be sent with Connection::send_message(), and received in Message::Direct() from
// Connection::recv()
#[derive(Clone, Debug)]
pub enum DirectMessage {
    AnnounceAccounts(Vec<AnnounceAccount>),
}

/// The types of messages it's possible to send/receive from a `Connection`. Any PeerMessage
/// we receive that doesn't fit one of the types we've enumerated will just be logged and dropped.
#[derive(Clone, Debug)]
pub enum Message {
    Direct(DirectMessage),
    Routed(RoutedMessage),
}

fn sprint_addr(addr: std::io::Result<SocketAddr>) -> String {
    match addr {
        Ok(addr) => addr.to_string(),
        Err(e) => format!("<socket addr unknown: {:?}>", e),
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "raw::Connection({:?}@{} -> {:?}@{})",
            &self.my_peer_id,
            sprint_addr(self.stream.local_addr()),
            &self.peer_id,
            sprint_addr(self.stream.peer_addr())
        )
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error(transparent)]
    IO(std::io::Error),
    #[error("handshake failed {0:?}")]
    HandshakeFailure(HandshakeFailureReason),
    #[error("received unexpected message before the handshake: {0:?}")]
    UnexpectedFirstMessage(PeerMessage),
}

impl Connection {
    /// Connect to the NEAR node at `peer_id`@`addr`. The inputs are used to build out handshake,
    /// and this function will return a `Peer` when a handshake has been received successfully.
    pub async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        my_protocol_version: Option<ProtocolVersion>,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
        recv_timeout: Duration,
    ) -> Result<Self, ConnectError> {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        let start = Instant::now();
        let stream =
            tokio::time::timeout(recv_timeout.try_into().unwrap(), TcpStream::connect(addr))
                .await
                .map_err(|e| ConnectError::IO(e.into()))?
                .map_err(ConnectError::IO)?;
        tracing::info!(
            target: "network", "Connection to {}@{:?} established. latency: {}",
            &peer_id, &addr, start.elapsed(),
        );

        let mut peer = Self {
            stream,
            peer_id,
            buf: BytesMut::with_capacity(1024),
            secret_key,
            my_peer_id,
            recv_timeout,
            // we set remove_frequent_min_size to 1 because the only peer ID we're ever
            // going to add to the cache is our own, so just remove one entry
            // at a time when the cache is full
            route_cache: RouteBackCache::new(100_000, time::Duration::seconds(1_000), 1),
            clock: Clock::real(),
        };
        peer.do_handshake(
            my_protocol_version.unwrap_or(PROTOCOL_VERSION),
            chain_id,
            genesis_hash,
            head_height,
        )
        .await?;

        Ok(peer)
    }

    async fn do_handshake(
        &mut self,
        protocol_version: ProtocolVersion,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
    ) -> Result<(), ConnectError> {
        let handshake = PeerMessage::Tier2Handshake(Handshake {
            protocol_version,
            oldest_supported_version: protocol_version - 2,
            sender_peer_id: self.my_peer_id.clone(),
            target_peer_id: self.peer_id.clone(),
            // we have to set this even if we have no intention of listening since otherwise
            // the peer will drop our connection
            sender_listen_port: Some(24567),
            sender_chain_info: PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            partial_edge_info: PartialEdgeInfo::new(
                &self.my_peer_id,
                &self.peer_id,
                1,
                &self.secret_key,
            ),
            owned_account: None,
        });

        self.write_message(&handshake).await.map_err(ConnectError::IO)?;

        let start = Instant::now();

        let (message, timestamp) = self.recv_message().await.map_err(ConnectError::IO)?;

        match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Tier2Handshake(_) => {
                tracing::info!(target: "network", "handshake latency: {}", timestamp - start);
            }
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => return Err(ConnectError::UnexpectedFirstMessage(message)),
        };

        Ok(())
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    // Try to send a PeerMessage corresponding to the given DirectMessage
    pub async fn send_message(&mut self, msg: DirectMessage) -> io::Result<()> {
        let peer_msg = match msg {
            DirectMessage::AnnounceAccounts(accounts) => {
                PeerMessage::SyncRoutingTable(RoutingTableUpdate { edges: Vec::new(), accounts })
            }
        };

        self.write_message(&peer_msg).await
    }

    // Try to send a routed PeerMessage corresponding to the given RoutedMessage
    pub async fn send_routed_message(
        &mut self,
        msg: RoutedMessage,
        target: PeerId,
        ttl: u8,
    ) -> io::Result<()> {
        let body = match msg {
            RoutedMessage::Ping { nonce } => {
                RoutedMessageBody::Ping(Ping { nonce, source: self.my_peer_id.clone() })
            }
            RoutedMessage::Pong { nonce, source } => {
                RoutedMessageBody::Pong(Pong { nonce, source })
            }
            RoutedMessage::StateRequestPart(shard_id, block_hash, part_id) => {
                RoutedMessageBody::StateRequestPart(shard_id, block_hash, part_id)
            }
            RoutedMessage::VersionedStateResponse(response) => {
                RoutedMessageBody::VersionedStateResponse(response)
            }
        };
        let msg = RawRoutedMessage { target: PeerIdOrHash::PeerId(target), body }.sign(
            &self.secret_key,
            ttl,
            Some(Utc::now_utc()),
        );
        self.route_cache.insert(&self.clock, msg.hash(), self.my_peer_id.clone());
        self.write_message(&PeerMessage::Routed(Box::new(msg))).await
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(
            self.recv_timeout.try_into().unwrap(),
            self.stream.read_buf(&mut self.buf),
        )
        .await??;
        tracing::trace!(target: "network", "Read {} bytes from {:?}", n, self.stream.peer_addr());
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no more bytes available, but expected to receive a message",
            ));
        }
        Ok(())
    }

    // read at least 4 bytes, but probably the whole message in most cases
    async fn read_msg_length(&mut self) -> io::Result<(usize, Instant)> {
        let mut first_byte_time = None;
        while self.buf.remaining() < 4 {
            self.do_read().await?;
            if first_byte_time.is_none() {
                first_byte_time = Some(Instant::now());
            }
        }
        let len = u32::from_le_bytes(self.buf[..4].try_into().unwrap());
        // If first_byte_time is None, there were already 4 bytes from last time,
        // and they must have come before a partial frame.
        // So the Instant::now() is not quite correct, since the time was really in the past,
        // but this is prob not so important
        Ok((len as usize, first_byte_time.unwrap_or(Instant::now())))
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    async fn recv_message(&mut self) -> io::Result<(PeerMessage, Instant)> {
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            self.do_read().await?;
        }

        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..msg_length]);
        self.buf.advance(msg_length);

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        msg.map(|m| {
            tracing::debug!(target: "network", "received PeerMessage::{} len: {}", &m, msg_length);
            (m, first_byte_time)
        })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("error parsing protobuf of length {}", msg_length),
            )
        })
    }

    fn target_is_for_me(&mut self, target: &PeerIdOrHash) -> bool {
        match target {
            PeerIdOrHash::PeerId(peer_id) => peer_id == &self.my_peer_id,
            PeerIdOrHash::Hash(hash) => {
                let target = self.route_cache.remove(&self.clock, hash);
                target.as_ref() == Some(&self.my_peer_id)
            }
        }
    }

    fn recv_routed_msg(
        &mut self,
        msg: &crate::network_protocol::RoutedMessage,
    ) -> Option<RoutedMessage> {
        if !self.target_is_for_me(&msg.target) {
            tracing::debug!(
                target: "network", "{:?} dropping routed message {} for {:?}",
                &self, <&'static str>::from(&msg.body), &msg.target
            );
            return None;
        }
        match &msg.body {
            RoutedMessageBody::Ping(p) => Some(RoutedMessage::Ping { nonce: p.nonce }),
            RoutedMessageBody::Pong(p) => {
                Some(RoutedMessage::Pong { nonce: p.nonce, source: p.source.clone() })
            }
            RoutedMessageBody::VersionedStateResponse(state_response_info) => {
                Some(RoutedMessage::VersionedStateResponse(state_response_info.clone()))
            }
            RoutedMessageBody::StateRequestPart(shard_id, hash, part_id) => {
                Some(RoutedMessage::StateRequestPart(*shard_id, *hash, *part_id))
            }
            _ => None,
        }
    }

    /// Reads from the socket until we receive some message that we care to pass to the caller
    /// (that is, represented in `DirectMessage` or `RoutedMessage`).
    pub async fn recv(&mut self) -> io::Result<(Message, Instant)> {
        loop {
            let (msg, timestamp) = self.recv_message().await?;
            match msg {
                PeerMessage::Routed(r) => {
                    if let Some(msg) = self.recv_routed_msg(&r) {
                        return Ok((Message::Routed(msg), timestamp));
                    }
                }
                PeerMessage::SyncRoutingTable(r) => {
                    return Ok((
                        Message::Direct(DirectMessage::AnnounceAccounts(r.accounts)),
                        timestamp,
                    ))
                }
                _ => {}
            }
        }
    }
}
