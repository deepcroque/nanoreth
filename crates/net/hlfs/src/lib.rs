//! HLFS TCP micro-protocol for historical backfill (single-block, RR per block).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use reth_tracing::tracing::trace;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    fs, io,
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tokio::fs::DirEntry;
use tracing::{debug, info, warn};

#[derive(Debug, Error)]
pub enum HlfsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("timeout")]
    Timeout,
    #[error("not found")]
    NotFound,
    #[error("busy {0:?}")]
    Busy(Duration),
    #[error("protocol")]
    Proto,
    #[error("no peers")]
    NoPeers,
    #[error("unknown")]
    Unknown,
}

#[inline]
fn put_u64(b: &mut BytesMut, v: u64) {
    b.put_u64_le(v)
}
#[inline]
fn put_u32(b: &mut BytesMut, v: u32) {
    b.put_u32_le(v)
}

async fn ensure_parent_dirs(path: &str) -> std::io::Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent).await
    } else {
        Ok(())
    }
}

/// Client: tries each peer once; rotates starting index per call.
#[derive(Clone)]
pub struct Client {
    peers: Arc<Mutex<Vec<SocketAddr>>>,
    timeout: Duration,
    backoff_ms: u32,
}
impl Client {
    pub fn new(peers: Vec<SocketAddr>) -> Self {
        Self { peers: Arc::new(Mutex::new(peers)), timeout: Duration::from_secs(5), backoff_ms: 50 }
    }
    pub fn update_peers(&self, peers: Vec<SocketAddr>) {
        *self.peers.lock() = peers;
    }
    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }
    pub async fn get_block(&self, number: u64, rr_index: usize) -> Result<Vec<u8>, HlfsError> {
        let peers = self.peers.lock().clone();
        debug!(peer_count = peers.len(), "hlfs: peers");
        if peers.is_empty() {
            return Err(HlfsError::NoPeers);
        }

        let mut all_not_found = true;
        let mut any_timeout = false;

        for t in 0..peers.len() {
            let i = (rr_index + t) % peers.len();
            let addr = peers[i];
            debug!(block=number, %addr, "hlfs: try");
            let start = std::time::Instant::now();
            match timeout(self.timeout, fetch_once(addr, number)).await {
                Err(_) => {
                    debug!(elapsed=?start.elapsed(), %addr, block=number, "hlfs: timeout");
                    any_timeout = true;
                    all_not_found = false;
                    continue;
                }
                Ok(Err(HlfsError::Busy(d))) => {
                    trace!(block=number, %addr, delay_ms=?d, "hlfs: busy");
                    sleep(d.min(Duration::from_millis(self.backoff_ms as u64))).await;
                    all_not_found = false;
                    continue;
                }
                Ok(Err(HlfsError::NotFound)) => {
                    trace!(block=number, %addr, "hlfs: not found");
                    // Keep all_not_found as true unless we see other errors
                    continue;
                }
                Ok(Err(e)) => {
                    debug!(block=number, %addr, error=%e, "hlfs: error");
                    all_not_found = false;
                    continue;
                }
                Ok(Ok(bytes)) => {
                    info!(block=number, %addr, bytes=bytes.len(), "hlfs: fetched");
                    return Ok(bytes);
                }
            }
        }

        // Return the most specific error
        if all_not_found {
            Err(HlfsError::NotFound)
        } else if any_timeout {
            Err(HlfsError::Timeout)
        } else {
            Err(HlfsError::Unknown) // Fallback for other errors
        }
    }
}

async fn fetch_once(addr: SocketAddr, number: u64) -> Result<Vec<u8>, HlfsError> {
    debug!(%addr, "hlfs: connect");
    let mut s = TcpStream::connect(addr).await?;
    debug!(%addr, "hlfs: CONNECTED");
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(0x01);
    put_u64(&mut buf, number);
    s.write_all(&buf).await?;
    let mut op = [0u8; 1];
    s.read_exact(&mut op).await?;
    debug!(code = op[0], "hlfs: opcode");
    match op[0] {
        0x02 => {
            let mut meta = [0u8; 12];
            s.read_exact(&mut meta).await?;
            let mut m = Bytes::from(meta.to_vec());
            let _n = m.get_u64_le();
            let len = m.get_u32_le() as usize;
            let mut data = vec![0u8; len];
            s.read_exact(&mut data).await?;
            Ok(data)
        }
        0x03 => {
            let mut _n = [0u8; 8];
            let _ = s.read_exact(&mut _n).await;
            Err(HlfsError::NotFound)
        }
        0x04 => {
            let mut d = [0u8; 4];
            s.read_exact(&mut d).await?;
            Err(HlfsError::Busy(Duration::from_millis(u32::from_le_bytes(d) as u64)))
        }
        _ => Err(HlfsError::Proto),
    }
}

fn parse_block_file(name: &str) -> Option<u64> {
    // expects "<number>.rmp.lz4"
    let (stem, ext) = name.rsplit_once('.')?;
    if ext != "lz4" {
        return None;
    }
    if !stem.ends_with(".rmp") {
        return None;
    }
    let stem = stem.strip_suffix(".rmp")?;
    stem.parse::<u64>().ok()
}

// Asynchronously find the largest block file under the 2-level shard layout:
// {root}/{floor_to_million}/{floor_to_thousand}/{number}.rmp.lz4
pub async fn find_max_block(root: &Path) -> io::Result<u64> {
    let mut max_num: Option<u64> = None;

    let mut top = fs::read_dir(root).await?;
    while let Some(million_dir) = top.next_entry().await? {
        if !million_dir.file_type().await?.is_dir() {
            continue;
        }
        // Fast reject: top-level dir must parse to u64 (but we still scan if not—optional)
        if million_dir.file_name().to_string_lossy().parse::<u64>().is_err() {
            continue;
        }

        let mut mid = fs::read_dir(million_dir.path()).await?;
        while let Some(thousand_dir) = mid.next_entry().await? {
            if !thousand_dir.file_type().await?.is_dir() {
                continue;
            }
            // Optional reject again for dir-name parse to u64
            if thousand_dir.file_name().to_string_lossy().parse::<u64>().is_err() {
                continue;
            }

            let mut leaf = fs::read_dir(thousand_dir.path()).await?;
            while let Some(ent) = leaf.next_entry().await? {
                if !ent.file_type().await?.is_file() {
                    continue;
                }
                if let Some(name) = ent.file_name().to_str() {
                    if let Some(n) = parse_block_file(name) {
                        if max_num.map_or(true, |m| n > m) {
                            max_num = Some(n);
                        }
                    }
                }
            }
        }
    }

    max_num.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no block files found"))
}

/// Server: serves `{root}/{number}.rlp`.
pub struct Server {
    bind: SocketAddr,
    root: PathBuf,
    max_conns: usize,
    inflight: Arc<Mutex<usize>>,
    busy_retry_ms: u32,
    max_block: u64,
}

impl Server {
    pub async fn new(bind: SocketAddr, root: impl Into<PathBuf>) -> Result<Self, HlfsError> {
        let root = root.into();
        fs::create_dir_all(&root).await?; // async, no unwrap/ok()
        let max_block = find_max_block(&root).await?; // async discovery

        Ok(Self {
            bind,
            root,
            max_conns: 512,
            inflight: Arc::new(parking_lot::Mutex::new(0)),
            busy_retry_ms: 100,
            max_block,
        })
    }

    pub fn with_limits(mut self, max_conns: usize, busy_retry_ms: u32) -> Self {
        self.max_conns = max_conns;
        self.busy_retry_ms = busy_retry_ms;
        self
    }
    pub async fn run(self) -> Result<(), HlfsError> {
        fs::create_dir_all(&self.root).await.ok();
        info!(%self.bind, root=%self.root.display(), max_conns=%self.max_conns, "hlfs: server listening");
        let lst = TcpListener::bind(self.bind).await?;
        loop {
            let (mut sock, addr) = lst.accept().await?;
            if *self.inflight.lock() >= self.max_conns {
                let mut b = BytesMut::with_capacity(5);
                b.put_u8(0x04);
                put_u32(&mut b, self.busy_retry_ms);
                let _ = sock.write_all(&b).await;
                continue;
            }
            *self.inflight.lock() += 1;
            let root = self.root.clone();
            let inflight = self.inflight.clone();
            let busy = self.busy_retry_ms;
            tokio::spawn(async move {
                let _ = handle_conn(&mut sock, &root, busy, addr, self.max_block).await;
                *inflight.lock() -= 1;
            });
        }
    }
}
async fn handle_conn(
    sock: &mut TcpStream,
    root: &Path,
    busy_ms: u32,
    addr: SocketAddr,
    max_block: u64,
) -> Result<(), HlfsError> {
    let mut op = [0u8; 1];
    sock.read_exact(&mut op).await?;
    if op[0] != 0x01 && op[0] != 0x02 {
        warn!(%addr, "hlfs: bad op");
        return Err(HlfsError::Proto);
    }
    if op[0] == 0x02 {
        let mut b = BytesMut::with_capacity(1 + 8 + 4);
        b.put_u8(0x05);
        put_u64(&mut b, max_block);
        return Ok(());
    }

    let mut nb = [0u8; 8];
    sock.read_exact(&mut nb).await?;
    let number = u64::from_le_bytes(nb);
    let n = number.saturating_sub(1); // 0 -> 0, others -> number-1
    let f = (n / 1_000_000) * 1_000_000;
    let s = (n / 1_000) * 1_000;
    let path = format!("{}/{f}/{s}/{number}.rmp.lz4", root.to_string_lossy());
    match fs::read(&path).await {
        Ok(data) => {
            debug!("hlfs: found path [{path}]");
            let mut b = BytesMut::with_capacity(1 + 8 + 4 + data.len());
            b.put_u8(0x02);
            put_u64(&mut b, number);
            put_u32(&mut b, data.len() as u32);
            b.extend_from_slice(&data);
            sock.write_all(&b).await?;
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut b = [0u8; 9];
            b[0] = 0x03;
            b[1..9].copy_from_slice(&number.to_le_bytes());
            sock.write_all(&b).await?;
            Ok(())
        }
        Err(_) => {
            let mut b = BytesMut::with_capacity(5);
            b.put_u8(0x04);
            put_u32(&mut b, busy_ms);
            let _ = sock.write_all(&b).await;
            Err(HlfsError::Io(io::Error::new(io::ErrorKind::Other, "fs error")))
        }
    }
}

/// Backfiller: ask client per missing block; rotate peers every block.
#[derive(Clone)]
pub struct Backfiller {
    client: Client,
    root: PathBuf,
    hist_threshold: u64,
}
impl Backfiller {
    pub fn new(client: Client, root: impl Into<PathBuf>, hist_threshold: u64) -> Self {
        Self { client, root: root.into(), hist_threshold }
    }
    pub fn set_peers(&self, peers: Vec<SocketAddr>) {
        self.client.update_peers(peers);
    }
    pub async fn fetch_if_missing(
        &self,
        number: u64,
        head: u64,
        rr_index: usize,
    ) -> Result<Option<usize>, HlfsError> {
        if head >= self.hist_threshold && number + self.hist_threshold > head {
            //debug!(block=number, "hlfs: skip");
            return Ok(None);
        }
        let n = number.saturating_sub(1); // 0 -> 0, others -> number-1
        let f = (n / 1_000_000) * 1_000_000;
        let s = (n / 1_000) * 1_000;

        let path = format!("{}/{f}/{s}/{number}.rmp.lz4", self.root.to_string_lossy());
        if fs::try_exists(&path).await? {
            return Ok(None);
        } else {
            ensure_parent_dirs(&path).await?;
        }

        debug!(block = number, "hlfs: going to get_block from client");
        match self.client.get_block(number, rr_index).await {
            Ok(bytes) => {
                debug!(block = number, "hlfs: YAY! got block from client");
                let tmp = format!("{}/{f}/{s}/{number}.rmp.lz4.part", self.root.to_string_lossy());
                ensure_parent_dirs(&tmp).await?;

                debug!(block = number, path=%tmp, "hlfs: writing file");
                fs::write(&tmp, &bytes).await?;
                debug!(block = number, from=%tmp, to=%path, "hlfs: moving file");
                fs::rename(&tmp, &path).await?;
                info!(block=number, bytes=bytes.len(), path=%path, "hlfs: wrote");
                Ok(Some(bytes.len()))
            }
            Err(e) => {
                debug!(block=number, error=%e, "hlfs: fetch failed");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    fn sample(n: u64) -> Vec<u8> {
        vec![((n as usize) % 251) as u8; 3072]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_and_fetch_rr() {
        reth_tracing::init_test_tracing();
        let dir = tempfile::tempdir().unwrap();
        for n in 0..100u64 {
            fs::write(dir.path().join(format!("{n}.rlp")), sample(n)).await.unwrap();
        }
        let s1 = Server::new("127.0.0.1:9597".parse().unwrap(), dir.path()).with_limits(64, 20);
        let s2 = Server::new("127.0.0.1:9598".parse().unwrap(), dir.path()).with_limits(64, 20);
        tokio::spawn(async move {
            let _ = s1.run().await;
        });
        tokio::spawn(async move {
            let _ = s2.run().await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client =
            Client::new(vec!["127.0.0.1:9597".parse().unwrap(), "127.0.0.1:9598".parse().unwrap()])
                .with_timeout(Duration::from_secs(1));
        let a = client.get_block(10, 0).await.unwrap();
        let b = client.get_block(11, 1).await.unwrap();
        assert_eq!(a.len(), 3072);
        assert_eq!(b.len(), 3072);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn backfill_only_when_older_than_threshold() {
        reth_tracing::init_test_tracing();
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        fs::write(src.path().join("5.rlp"), sample(5)).await.unwrap();
        let srv = Server::new("127.0.0.1:9599".parse().unwrap(), src.path());
        tokio::spawn(async move {
            let _ = srv.run().await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let bf = Backfiller::new(
            Client::new(vec!["127.0.0.1:9599".parse().unwrap()]),
            dst.path(),
            5_000,
        );
        let got = bf.fetch_if_missing(5, 10_000, 0).await.unwrap();
        assert_eq!(got, Some(3072));
        let skip = bf.fetch_if_missing(9_999, 10_000, 1).await.unwrap();
        assert_eq!(skip, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn busy_and_notfound_rotate() {
        reth_tracing::init_test_tracing();
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("7.rlp"), sample(7)).await.unwrap();
        let s_busy = Server::new("127.0.0.1:9601".parse().unwrap(), dir.path()).with_limits(0, 10);
        let s_ok = Server::new("127.0.0.1:9602".parse().unwrap(), dir.path());
        tokio::spawn(async move {
            let _ = s_busy.run().await;
        });
        tokio::spawn(async move {
            let _ = s_ok.run().await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let c =
            Client::new(vec!["127.0.0.1:9601".parse().unwrap(), "127.0.0.1:9602".parse().unwrap()]);
        let b = c.get_block(7, 0).await.unwrap();
        assert_eq!(b.len(), 3072);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_peers_return_not_found() {
        reth_tracing::init_test_tracing();
        let dir = tempfile::tempdir().unwrap();
        // Don't create the block file, so all servers will return NotFound

        let s1 = Server::new("127.0.0.1:9603".parse().unwrap(), dir.path());
        let s2 = Server::new("127.0.0.1:9604".parse().unwrap(), dir.path());
        tokio::spawn(async move {
            let _ = s1.run().await;
        });
        tokio::spawn(async move {
            let _ = s2.run().await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client =
            Client::new(vec!["127.0.0.1:9603".parse().unwrap(), "127.0.0.1:9604".parse().unwrap()])
                .with_timeout(Duration::from_secs(1));

        // Request a block that doesn't exist on any peer
        let result = client.get_block(999, 0).await;
        assert!(matches!(result, Err(HlfsError::NotFound)));
    }
}
