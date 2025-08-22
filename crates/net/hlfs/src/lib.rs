//! HLFS TCP micro-protocol for historical backfill (single-block, RR per block).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use reth_tracing::tracing::{debug, info, trace, warn};
use std::{
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

type Result<T, E = HlfsError> = std::result::Result<T, E>;

#[derive(Error, Debug)]
pub enum HlfsError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("proto")]
    Proto,
    #[error("no peers")]
    NoPeers,
    #[error("timeout")]
    Timeout,
    #[error("busy: retry_ms={0}")]
    Busy(u32),
    #[error("not found")]
    NotFound,
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
        fs::create_dir_all(parent)
    } else {
        Ok(())
    }
}

/// Client: tries each peer once; rotates starting index per call
#[derive(Clone)]
pub struct Client {
    peers: Arc<Mutex<Vec<SocketAddr>>>,
    timeout: Duration,
}
impl Client {
    pub fn new(peers: Vec<SocketAddr>) -> Self {
        Self { peers: Arc::new(Mutex::new(peers)), timeout: Duration::from_secs(3) }
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

        let mut all = (0..peers.len()).map(|i| (rr_index + i) % peers.len());
        let mut last_busy: Option<u32> = None;
        while let Some(i) = all.next() {
            let addr = peers[i];
            trace!(%addr, "hlfs: dialing");
            match timeout(self.timeout, TcpStream::connect(addr)).await {
                Err(_) => continue,
                Ok(Err(_)) => continue,
                Ok(Ok(mut sock)) => {
                    let mut req = BytesMut::with_capacity(1 + 8);
                    req.put_u8(0x01); // GET
                    put_u64(&mut req, number);
                    if let Err(e) = sock.write_all(&req).await {
                        debug!(%addr, "hlfs: write err: {e}");
                        continue;
                    }
                    let mut op = [0u8; 1];
                    if let Err(e) = timeout(self.timeout, sock.read_exact(&mut op)).await {
                        debug!(%addr, "hlfs: read op timeout {e:?}");
                        continue;
                    }
                    let op = op[0];
                    match op {
                        0x03 => {
                            // DATA
                            let mut len = [0u8; 4];
                            sock.read_exact(&mut len).await?;
                            let len = u32::from_le_bytes(len) as usize;
                            let mut buf = vec![0u8; len];
                            sock.read_exact(&mut buf).await?;
                            return Ok(buf);
                        }
                        0x04 => {
                            let mut ms = [0u8; 4];
                            sock.read_exact(&mut ms).await?;
                            last_busy = Some(u32::from_le_bytes(ms));
                            continue;
                        }
                        0x06 => {
                            return Err(HlfsError::NotFound);
                        }
                        _ => {
                            continue;
                        }
                    }
                }
            }
        }
        if let Some(ms) = last_busy {
            return Err(HlfsError::Busy(ms));
        }
        Err(HlfsError::NotFound)
    }
}

fn find_max_number_file(root: &Path) -> Result<u64> {
    fn parse_num(name: &str) -> Option<u64> {
        name.strip_suffix(".rmp.lz4")?.parse::<u64>().ok()
    }

    fn walk(dir: &Path, best: &mut Option<u64>) -> io::Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(&path, best)?;
            } else if ft.is_file() {
                if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                    if let Some(n) = parse_num(name) {
                        if best.map_or(true, |b| n > b) {
                            *best = Some(n);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    let mut best = None;
    walk(root, &mut best)?;
    Ok(best.expect("cannot find block files"))
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
    pub fn new(bind: SocketAddr, root: impl Into<PathBuf>) -> Self {
        let root: PathBuf = root.into();
        let n = find_max_number_file(&root).unwrap();
        Self {
            bind,
            root,
            max_conns: 512,
            inflight: Arc::new(Mutex::new(0)),
            busy_retry_ms: 100,
            max_block: n,
        }
    }
    pub fn with_limits(mut self, max_conns: usize, busy_retry_ms: u32) -> Self {
        self.max_conns = max_conns;
        self.busy_retry_ms = busy_retry_ms;
        self
    }
    pub async fn run(self) -> Result<(), HlfsError> {
        fs::create_dir_all(&self.root).ok();
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
        put_u32(&mut b, busy_ms);
        let _ = sock.write_all(&b).await;
        return Ok(());
    }

    let mut num = [0u8; 8];
    sock.read_exact(&mut num).await?;
    let number = u64::from_le_bytes(num);

    let n = number.saturating_sub(1); // 0 -> 0, others -> number-1
    let f = (n / 1_000_000) * 1_000_000;
    let s = (n / 1_000) * 1_000;
    let path = format!("{}/{f}/{s}/{number}.rmp.lz4", root.to_string_lossy());

    trace!(%addr, number, %path, "hlfs: req");
    if let Err(e) = ensure_parent_dirs(&path).await {
        warn!(%addr, %path, "hlfs: mkdirs failed: {e}");
    }

    match fs::read(&path) {
        Ok(data) => {
            let mut b = BytesMut::with_capacity(1 + 4 + data.len());
            b.put_u8(0x03);
            put_u32(&mut b, data.len() as u32);
            b.extend_from_slice(&data);
            let _ = sock.write_all(&b).await;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut b = BytesMut::with_capacity(1);
            b.put_u8(0x06); // not found
            let _ = sock.write_all(&b).await;
        }
        Err(e) => {
            warn!(%addr, %path, "hlfs: read error: {e}");
            let _ = sock.shutdown().await;
        }
    }
    Ok(())
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
        if Path::new(&path).exists() {
            trace!(block = number, "hlfs: already have");
            return Ok(None);
        }
        match self.client.get_block(number, rr_index).await {
            Err(HlfsError::NotFound) => Ok(None),
            Err(HlfsError::Busy(ms)) => {
                tokio::time::sleep(Duration::from_millis(ms as u64)).await;
                Ok(None)
            }
            Err(e) => Err(e),
            Ok(data) => {
                if let Err(e) = ensure_parent_dirs(&path).await {
                    warn!(%path, "hlfs: mkdirs failed: {e}");
                }
                if let Err(e) = fs::write(&path, &data) {
                    warn!(%path, "hlfs: write failed: {e}");
                    return Ok(None);
                }
                Ok(Some(data.len()))
            }
        }
    }
}
