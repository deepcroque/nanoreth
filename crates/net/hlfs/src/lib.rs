//! HLFS TCP micro-protocol for historical backfill (single-block, RR per block).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use reth_tracing::tracing::trace;
use std::{
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
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
}

#[inline]
fn put_u64(b: &mut BytesMut, v: u64) {
    b.put_u64_le(v)
}
#[inline]
fn put_u32(b: &mut BytesMut, v: u32) {
    b.put_u32_le(v)
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
        if peers.is_empty() {
            debug!(block = number, "hlfs: no peers");
            return Err(HlfsError::Timeout);
        }
        for t in 0..peers.len() {
            let i = (rr_index + t) % peers.len();
            let addr = peers[i];
            trace!(block=number, %addr, "hlfs: try");
            match timeout(self.timeout, fetch_once(addr, number)).await {
                Err(_) => {
                    debug!(block=number, %addr, "hlfs: timeout");
                    continue;
                }
                Ok(Err(HlfsError::Busy(d))) => {
                    trace!(block=number, %addr, delay_ms=?d, "hlfs: busy");
                    sleep(d.min(Duration::from_millis(self.backoff_ms as u64))).await;
                    continue;
                }
                Ok(Err(HlfsError::NotFound)) => {
                    trace!(block=number, %addr, "hlfs: not found");
                    continue;
                }
                Ok(Err(e)) => {
                    debug!(block=number, %addr, error=%e, "hlfs: error");
                    continue;
                }
                Ok(Ok(bytes)) => {
                    info!(block=number, %addr, bytes=bytes.len(), "hlfs: fetched");
                    return Ok(bytes);
                }
            }
        }
        Err(HlfsError::Timeout)
    }
}
async fn fetch_once(addr: SocketAddr, number: u64) -> Result<Vec<u8>, HlfsError> {
    let mut s = TcpStream::connect(addr).await?;
    let mut buf = BytesMut::with_capacity(9);
    buf.put_u8(0x01);
    put_u64(&mut buf, number);
    s.write_all(&buf).await?;
    let mut op = [0u8; 1];
    s.read_exact(&mut op).await?;
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

/// Server: serves `{root}/{number}.rlp`.
pub struct Server {
    bind: SocketAddr,
    root: PathBuf,
    max_conns: usize,
    inflight: Arc<Mutex<usize>>,
    busy_retry_ms: u32,
}
impl Server {
    pub fn new(bind: SocketAddr, root: impl Into<PathBuf>) -> Self {
        Self {
            bind,
            root: root.into(),
            max_conns: 512,
            inflight: Arc::new(Mutex::new(0)),
            busy_retry_ms: 100,
        }
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
                let _ = handle_conn(&mut sock, &root, busy, addr).await;
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
) -> Result<(), HlfsError> {
    let mut op = [0u8; 1];
    sock.read_exact(&mut op).await?;
    if op[0] != 0x01 {
        warn!(%addr, "hlfs: bad op");
        return Err(HlfsError::Proto);
    }
    let mut nb = [0u8; 8];
    sock.read_exact(&mut nb).await?;
    let number = u64::from_le_bytes(nb);
    let path = root.join(format!("{number}.rlp"));
    match fs::read(&path).await {
        Ok(data) => {
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
        if number + self.hist_threshold > head {
            return Ok(None);
        }
        let path = self.root.join(format!("{number}.rlp"));
        if fs::try_exists(&path).await? {
            return Ok(None);
        }
        match self.client.get_block(number, rr_index).await {
            Ok(bytes) => {
                let tmp = self.root.join(format!("{number}.rlp.part"));
                fs::write(&tmp, &bytes).await?;
                fs::rename(&tmp, &path).await?;
                info!(block=number, bytes=bytes.len(), path=%path.display(), "hlfs: wrote");
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
}
