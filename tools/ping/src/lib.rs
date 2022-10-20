use anyhow::Context;
use bytes::buf::{Buf, BufMut};
use bytes::BytesMut;
use near_crypto::{KeyType, SecretKey};
use near_network::types::{Encoding, Handshake, HandshakeFailureReason, PeerMessage};
use near_network_primitives::time::Utc;
use near_network_primitives::types::{
    AccountOrPeerIdOrHash, PartialEdgeInfo, PeerChainInfoV2, Ping, RawRoutedMessage,
    RoutedMessageBody,
};
use near_primitives::block::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::types::{AccountId, BlockHeight, EpochId};
use near_primitives::version::{ProtocolVersion, PROTOCOL_VERSION};
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub mod csv;

// TODO: also log number of bytes/other messages (like Blocks) received?
#[derive(Debug, Default)]
pub struct PingStats {
    pub pings_sent: usize,
    pub pongs_received: usize,
    // TODO: these latency stats could be separated into time to first byte
    // + time to last byte, etc.
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub average_latency: Duration,
}

impl PingStats {
    fn pong_received(&mut self, latency: Duration) {
        self.pongs_received += 1;

        if self.min_latency == Duration::ZERO || self.min_latency > latency {
            self.min_latency = latency;
        }
        if self.max_latency < latency {
            self.max_latency = latency;
        }
        let n = self.pongs_received as u32;
        self.average_latency = ((n - 1) * self.average_latency + latency) / n;
    }
}

pub struct Peer {
    my_peer_id: PeerId,
    peer_id: PeerId,
    secret_key: SecretKey,
    stream: TcpStream,
    buf: BytesMut,
}

pub enum Message {
    AnnounceAccounts(Vec<(AccountId, PeerId, EpochId)>),
    Pong { nonce: u64, source: PeerId },
}

impl Message {
    fn from_peer_message(m: PeerMessage) -> Option<Self> {
        match m {
            PeerMessage::Routed(r) => match &r.body {
                RoutedMessageBody::Pong(p) => {
                    Some(Self::Pong { nonce: p.nonce, source: p.source.clone() })
                }
                _ => None,
            },
            PeerMessage::SyncRoutingTable(r) => Some(Self::AnnounceAccounts(
                r.accounts.into_iter().map(|a| (a.account_id, a.peer_id, a.epoch_id)).collect(),
            )),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let addr = match self.stream.peer_addr() {
            Ok(a) => format!("{:?}", a),
            Err(e) => format!("Err({:?})", e),
        };
        write!(f, "{:?}@{:?}", &self.peer_id, addr)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("handshake failed {0:?}")]
    HandshakeFailure(HandshakeFailureReason),
}

impl Peer {
    pub async fn connect(
        addr: SocketAddr,
        peer_id: PeerId,
        my_protocol_version: Option<ProtocolVersion>,
        chain_id: &str,
        genesis_hash: CryptoHash,
        head_height: BlockHeight,
    ) -> Result<Self, ConnectError> {
        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let my_peer_id = PeerId::new(secret_key.public_key());

        let start = Instant::now();
        let stream = tokio::time::timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .map_err(Into::<io::Error>::into)??;
        tracing::info!(
            target: "ping", "Connection to {:?}@{:?} established. latency: {:?}",
            &peer_id, &addr, start.elapsed(),
        );

        let mut peer =
            Self { stream, peer_id, buf: BytesMut::with_capacity(1024), secret_key, my_peer_id };
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
        let handshake = PeerMessage::Handshake(Handshake::new(
            protocol_version,
            self.my_peer_id.clone(),
            self.peer_id.clone(),
            // we have to set this even if we have no intention of listening since otherwise
            // the peer will drop our connection
            Some(24567),
            PeerChainInfoV2 {
                genesis_id: GenesisId { chain_id: chain_id.to_string(), hash: genesis_hash },
                height: head_height,
                tracked_shards: vec![0],
                archival: false,
            },
            PartialEdgeInfo::new(&self.my_peer_id, &self.peer_id, 1, &self.secret_key),
        ));

        self.write_message(&handshake).await?;

        let start = Instant::now();

        let (message, timestamp) = self.recv_message().await?;

        match message {
            // TODO: maybe check the handshake for sanity
            PeerMessage::Handshake(_) => {
                tracing::info!(target: "ping", "handshake latency: {:?}", timestamp - start);
            }
            PeerMessage::HandshakeFailure(_peer_info, reason) => {
                return Err(ConnectError::HandshakeFailure(reason))
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "First message received from {:?} is not a handshake: {:?}",
                        self, &message
                    ),
                )
                .into())
            }
        };

        Ok(())
    }

    async fn write_message(&mut self, msg: &PeerMessage) -> io::Result<()> {
        let mut msg = msg.serialize(Encoding::Proto);
        let mut buf = (msg.len() as u32).to_le_bytes().to_vec();
        buf.append(&mut msg);
        self.stream.write_all(&buf).await
    }

    // panics if there's not at least `len` + 4 bytes available
    fn extract_msg(&mut self, len: usize) -> io::Result<PeerMessage> {
        self.buf.advance(4);
        let msg = PeerMessage::deserialize(Encoding::Proto, &self.buf[..len]);
        self.buf.advance(len);
        msg.map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("error parsing protobuf of length {}", len),
            )
        })
    }

    async fn do_read(&mut self) -> io::Result<()> {
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read_buf(&mut self.buf))
            .await??;
        tracing::trace!(target: "ping", "Read {} bytes from {:?}", n, self.stream.peer_addr());
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

    // drains anything still available to be read without blocking and
    // returns a vec of message lengths currently ready
    fn read_remaining_messages(
        &mut self,
        first_msg_length: usize,
    ) -> io::Result<Vec<(usize, Instant)>> {
        let mut pos = first_msg_length + 4;
        let mut lengths = Vec::new();
        let mut pending_msg_length = None;

        loop {
            match self.stream.try_read_buf(&mut self.buf) {
                Ok(n) => {
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "no more bytes available, but expected to receive a message",
                        ));
                    }
                    let timestamp = Instant::now();
                    tracing::trace!(target: "ping", "Read {} bytes from {:?} non-blocking", n, self.stream.peer_addr());
                    loop {
                        if pending_msg_length.is_none() && self.buf.remaining() - pos >= 4 {
                            let len =
                                u32::from_le_bytes(self.buf[pos..pos + 4].try_into().unwrap());
                            pending_msg_length = Some(len as usize);
                        }
                        if let Some(l) = pending_msg_length {
                            if self.buf.remaining() - pos >= l + 4 {
                                lengths.push((l, timestamp));
                                pos += l + 4;
                                pending_msg_length = None;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }
                Err(e) => match e.kind() {
                    io::ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => return Err(e),
                },
            };
        }
        Ok(lengths)
    }

    // Reads from the socket until there is at least one full PeerMessage available.
    // After that point, continues to read any bytes available to be read without blocking
    // and appends any extra messages to the returned Vec. Usually there is only one in there
    async fn recv_messages(&mut self) -> io::Result<Vec<(PeerMessage, Instant)>> {
        let mut messages = Vec::new();
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            // TODO: measure time to last byte here
            self.do_read().await?;
        }

        let more_messages = self.read_remaining_messages(msg_length)?;

        let msg = self.extract_msg(msg_length)?;
        tracing::debug!(target: "ping", "received PeerMessage::{} len: {}", msg, msg_length);
        messages.push((msg, first_byte_time));

        for (len, timestamp) in more_messages {
            let msg = self.extract_msg(len)?;
            tracing::debug!(target: "ping", "received PeerMessage::{} len: {}", msg, len);
            messages.push((msg, timestamp));
        }

        // make sure we can probably read the next message in one syscall next time
        let max_len_after_next_read = self.buf.chunk_mut().len() + self.buf.remaining();
        if max_len_after_next_read < 512 {
            self.buf.reserve(512 - max_len_after_next_read);
        }
        Ok(messages)
    }

    async fn recv_message(&mut self) -> io::Result<(PeerMessage, Instant)> {
        let (msg_length, first_byte_time) = self.read_msg_length().await?;

        while self.buf.remaining() < msg_length + 4 {
            self.do_read().await?;
        }
        Ok((self.extract_msg(msg_length)?, first_byte_time))
    }

    pub async fn recv(&mut self) -> io::Result<Vec<(Message, Instant)>> {
        loop {
            let msgs = self.recv_messages().await?;
            let mut ret = Vec::new();

            for (msg, timestamp) in msgs {
                if let Some(m) = Message::from_peer_message(msg) {
                    ret.push((m, timestamp));
                }
            }
            if !ret.is_empty() {
                return Ok(ret);
            }
        }
    }

    pub async fn send_ping(&mut self, target: &PeerId, nonce: u64, ttl: u8) -> anyhow::Result<()> {
        let body = RoutedMessageBody::Ping(Ping { nonce, source: self.my_peer_id.clone() });
        let msg = RawRoutedMessage { target: AccountOrPeerIdOrHash::PeerId(target.clone()), body }
            .sign(self.my_peer_id.clone(), &self.secret_key, ttl, Some(Utc::now_utc()));

        self.write_message(&PeerMessage::Routed(Box::new(msg))).await?;

        Ok(())
    }
}

type Nonce = u64;

#[derive(Debug, Eq, PartialEq)]
struct PingTarget {
    peer_id: PeerId,
    last_pinged: Option<Instant>,
}

impl PartialOrd for PingTarget {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTarget {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match &self.last_pinged {
            Some(my_last_pinged) => match &other.last_pinged {
                Some(their_last_pinged) => my_last_pinged
                    .cmp(their_last_pinged)
                    .then_with(|| self.peer_id.cmp(&other.peer_id)),
                None => cmp::Ordering::Greater,
            },
            None => match &other.last_pinged {
                Some(_) => cmp::Ordering::Less,
                None => self.peer_id.cmp(&other.peer_id),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PingTimeout {
    peer_id: PeerId,
    nonce: u64,
    timeout: Instant,
}

impl PartialOrd for PingTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PingTimeout {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.timeout.cmp(&other.timeout).then_with(|| {
            self.nonce.cmp(&other.nonce).then_with(|| self.peer_id.cmp(&other.peer_id))
        })
    }
}

fn peer_str(peer_id: &PeerId, account_id: Option<&AccountId>) -> String {
    account_id.map_or_else(|| format!("{}", peer_id), |a| format!("{}", a))
}

const MAX_PINGS_IN_FLIGHT: usize = 10;
const PING_TIMEOUT: Duration = Duration::from_secs(100);

#[derive(Debug)]
struct PingState {
    stats: PingStats,
    last_pinged: Option<Instant>,
    account_id: Option<AccountId>,
}

struct PingTimes {
    sent_at: Instant,
    timeout: Instant,
}

struct AppInfo {
    stats: HashMap<PeerId, PingState>,
    // we will ping targets in round robin fashion. So this keeps a set of
    // targets ordered by when we last pinged them.
    requests: BTreeMap<PingTarget, HashMap<Nonce, PingTimes>>,
    timeouts: BTreeSet<PingTimeout>,
    account_filter: Option<HashSet<AccountId>>,
}

impl AppInfo {
    fn new(account_filter: Option<HashSet<AccountId>>) -> Self {
        Self {
            stats: HashMap::new(),
            requests: BTreeMap::new(),
            timeouts: BTreeSet::new(),
            account_filter,
        }
    }

    fn pick_next_target(&self) -> Option<PeerId> {
        for (target, pending_pings) in self.requests.iter() {
            if pending_pings.len() < MAX_PINGS_IN_FLIGHT {
                return Some(target.peer_id.clone());
            }
        }
        None
    }

    fn ping_sent(&mut self, peer_id: &PeerId, nonce: u64) {
        let timestamp = Instant::now();
        let timeout = timestamp + PING_TIMEOUT;

        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                let mut pending_pings = self
                    .requests
                    .remove(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                println!(
                    "send ping --------------> {}",
                    peer_str(&peer_id, state.account_id.as_ref())
                );

                state.stats.pings_sent += 1;
                state.last_pinged = Some(timestamp);

                match pending_pings.entry(nonce) {
                    Entry::Occupied(_) => {
                        tracing::warn!(
                            target: "ping", "Internal error! Sent two pings with nonce {} to {}. \
                            Latency stats will probably be wrong.", nonce, &peer_id
                        );
                    }
                    Entry::Vacant(e) => {
                        e.insert(PingTimes { sent_at: timestamp, timeout });
                    }
                };
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: Some(timestamp) },
                    pending_pings,
                );
                self.timeouts.insert(PingTimeout { peer_id: peer_id.clone(), nonce, timeout })
            }
            Entry::Vacant(_) => {
                panic!("sent ping to {:?}, but not present in stats HashMap", peer_id)
            }
        };
    }

    fn pong_received(
        &mut self,
        peer_id: &PeerId,
        nonce: u64,
        received_at: Instant,
    ) -> Option<(Duration, Option<&AccountId>)> {
        match self.stats.get_mut(peer_id) {
            Some(state) => {
                let pending_pings = self
                    .requests
                    .get_mut(&PingTarget {
                        peer_id: peer_id.clone(),
                        last_pinged: state.last_pinged,
                    })
                    .unwrap();

                match pending_pings.remove(&nonce) {
                    Some(times) => {
                        let latency = received_at - times.sent_at;
                        state.stats.pong_received(latency);
                        assert!(self.timeouts.remove(&PingTimeout {
                            peer_id: peer_id.clone(),
                            nonce,
                            timeout: times.timeout
                        }));

                        println!(
                            "recv pong <-------------- {} latency: {:?}",
                            peer_str(&peer_id, state.account_id.as_ref()),
                            latency
                        );
                        Some((latency, state.account_id.as_ref()))
                    }
                    None => {
                        tracing::warn!(
                            target: "ping",
                            "received pong with nonce {} from {}, after we probably treated it as timed out previously",
                            nonce, peer_str(&peer_id, state.account_id.as_ref())
                        );
                        None
                    }
                }
            }
            None => {
                tracing::warn!(target: "ping", "received pong from {:?}, but don't know of this peer", peer_id);
                None
            }
        }
    }

    fn pop_timeout(&mut self, t: &PingTimeout) {
        assert!(self.timeouts.remove(&t));
        let state = self.stats.get(&t.peer_id).unwrap();

        let pending_pings = self
            .requests
            .get_mut(&PingTarget {
                peer_id: t.peer_id.clone(),
                last_pinged: state.last_pinged.clone(),
            })
            .unwrap();
        assert!(pending_pings.remove(&t.nonce).is_some());
        println!(
            "{} timeout after {:?} ---------",
            peer_str(&t.peer_id, state.account_id.as_ref()),
            PING_TIMEOUT
        );
    }

    fn add_peer(&mut self, peer_id: &PeerId, account_id: Option<&AccountId>) {
        if let Some(filter) = self.account_filter.as_ref() {
            if let Some(account_id) = account_id {
                if !filter.contains(account_id) {
                    tracing::debug!(target: "ping", "skipping AnnounceAccount for {}", account_id);
                    return;
                }
            }
        }
        match self.stats.entry(peer_id.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(account_id) = account_id {
                    let state = e.get_mut();
                    if let Some(old) = state.account_id.as_ref() {
                        if old != account_id {
                            // TODO: we should just keep track of all accounts that map
                            // to this peer id, since it's valid for there to be more than one.
                            // We only use the accounts in the account filter and when displaying
                            // ping targets, so theres no reason we cant keep track of all of them
                            tracing::warn!(
                                target: "ping", "Received Announce Account mapping {:?} to {:?}, but already \
                                knew of account id {:?}. Keeping old value",
                                peer_id, account_id, old
                            );
                        }
                    } else {
                        state.account_id = Some(account_id.clone());
                    }
                }
            }
            Entry::Vacant(e) => {
                e.insert(PingState {
                    account_id: account_id.cloned(),
                    last_pinged: None,
                    stats: PingStats::default(),
                });
                self.requests.insert(
                    PingTarget { peer_id: peer_id.clone(), last_pinged: None },
                    HashMap::new(),
                );
            }
        }
    }

    fn add_announce_accounts(&mut self, accounts: &[(AccountId, PeerId, EpochId)]) {
        for (account_id, peer_id, _epoch_id) in accounts.iter() {
            self.add_peer(peer_id, Some(account_id));
        }
    }

    fn peer_id_to_account_id(&self, peer_id: &PeerId) -> Option<&AccountId> {
        self.stats.get(peer_id).and_then(|s| s.account_id.as_ref())
    }
}

fn handle_message(
    app_info: &mut AppInfo,
    msg: &Message,
    received_at: Instant,
    latencies_csv: Option<&mut crate::csv::LatenciesCsv>,
) -> anyhow::Result<()> {
    match &msg {
        Message::Pong { nonce, source } => {
            if let Some((latency, account_id)) = app_info.pong_received(source, *nonce, received_at)
            {
                if let Some(csv) = latencies_csv {
                    csv.write(source, account_id, latency).context("Failed writing to CSV file")?;
                }
            }
        }
        Message::AnnounceAccounts(a) => {
            app_info.add_announce_accounts(a);
        }
    };
    Ok(())
}

#[derive(Debug)]
pub struct PeerIdentifier {
    pub account_id: Option<AccountId>,
    pub peer_id: PeerId,
}

impl std::fmt::Display for PeerIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.account_id {
            Some(a) => a.fmt(f),
            None => self.peer_id.fmt(f),
        }
    }
}

fn collect_stats(app_info: AppInfo) -> Vec<(PeerIdentifier, PingStats)> {
    let mut ret = Vec::new();
    for (peer_id, state) in app_info.stats {
        let PingState { stats, account_id, .. } = state;
        ret.push((PeerIdentifier { peer_id, account_id }, stats));
    }
    ret
}

fn prepare_timeout(sleep: Pin<&mut tokio::time::Sleep>, app_info: &AppInfo) -> Option<PingTimeout> {
    if let Some(t) = app_info.timeouts.iter().next() {
        sleep.reset(tokio::time::Instant::from_std(t.timeout));
        Some(t.clone())
    } else {
        None
    }
}

pub async fn ping_via_node(
    chain_id: &str,
    genesis_hash: CryptoHash,
    head_height: BlockHeight,
    protocol_version: Option<ProtocolVersion>,
    peer_id: PeerId,
    peer_addr: SocketAddr,
    ttl: u8,
    ping_frequency_millis: u64,
    account_filter: Option<HashSet<AccountId>>,
    mut latencies_csv: Option<crate::csv::LatenciesCsv>,
) -> Vec<(PeerIdentifier, PingStats)> {
    let mut app_info = AppInfo::new(account_filter);

    app_info.add_peer(&peer_id, None);

    let mut peer = match Peer::connect(
        peer_addr,
        peer_id,
        protocol_version,
        chain_id,
        genesis_hash,
        head_height,
    )
    .await
    {
        Ok(p) => p,
        Err(ConnectError::IO(e)) => {
            tracing::error!(target: "ping", "Error connecting to {:?}: {}", peer_addr, e);
            return vec![];
        }
        Err(ConnectError::HandshakeFailure(reason)) => {
            match reason {
                HandshakeFailureReason::ProtocolVersionMismatch { version, oldest_supported_version } => tracing::error!(
                    "Received Handshake Failure: {:?}. Try running again with --protocol-version between {} and {}",
                    reason, oldest_supported_version, version
                ),
                HandshakeFailureReason::GenesisMismatch(_) => tracing::error!(
                    "Received Handshake Failure: {:?}. Try running again with --chain-id and --genesis-hash set to these values.",
                    reason,
                ),
                HandshakeFailureReason::InvalidTarget => tracing::error!(
                    "Received Handshake Failure: {:?}. Is the public key given with --peer correct?",
                    reason,
                ),
            }
            return vec![];
        }
    };

    let mut nonce = 1;
    let next_ping = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next_ping);
    let next_timeout = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(next_timeout);

    loop {
        let target = app_info.pick_next_target();
        let pending_timeout = prepare_timeout(next_timeout.as_mut(), &app_info);

        tokio::select! {
            _ = &mut next_ping, if target.is_some() => {
                let target = target.unwrap();
                if let Err(e) = peer.send_ping(&target, nonce, ttl).await {
                    tracing::error!(target: "ping", "Failed sending ping to {:?}: {:#}", &target, e);
                    break;
                }
                app_info.ping_sent(&target, nonce);
                nonce += 1;
                next_ping.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(ping_frequency_millis));
            }
            res = peer.recv() => {
                let messages = match res {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(target: "ping", "Failed receiving messages: {:#}", e);
                        break;
                    }
                };
                for (msg, first_byte_time) in messages {
                    if let Err(e) = handle_message(&mut app_info, &msg, first_byte_time, latencies_csv.as_mut()) {
                        tracing::error!(target: "ping", "{:#}", e);
                        break;
                    }
                }
            }
            _ = &mut next_timeout, if pending_timeout.is_some() => {
                let t = pending_timeout.unwrap();
                app_info.pop_timeout(&t);
                if let Some(csv) = latencies_csv.as_mut() {
                    if let Err(e) =
                    csv.write_timeout(&t.peer_id, app_info.peer_id_to_account_id(&t.peer_id)) {
                        tracing::error!("Failed writing to CSV file: {}", e);
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }
    collect_stats(app_info)
}
