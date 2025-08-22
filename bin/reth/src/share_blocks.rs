use clap::Args;
use reth_hlfs::{Backfiller, Client, PeerRecord, Server, OP_REQ_MAX_BLOCK, OP_RES_MAX_BLOCK};
use reth_network_api::{events::NetworkEvent, FullNetwork};
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    task::JoinHandle,
    time::{sleep, timeout, Duration},
};
use tracing::{debug, info, warn};

// use futures_util::StreamExt;
use futures_util::stream::StreamExt;

#[derive(Args, Clone, Debug)]
pub(crate) struct ShareBlocksArgs {
    #[arg(long, default_value_t = false)]
    pub share_blocks: bool,
    #[arg(long, default_value = "0.0.0.0")]
    pub share_blocks_host: String,
    #[arg(long, default_value_t = 9595)]
    pub share_blocks_port: u16,
    #[arg(long, default_value = "evm-blocks")]
    pub archive_dir: PathBuf,
}

pub(crate) struct ShareBlocks {
    _server: JoinHandle<()>,
    _autodetect: JoinHandle<()>,
}

impl ShareBlocks {
    pub(crate) async fn start_with_network<Net>(
        args: &ShareBlocksArgs,
        network: Net,
    ) -> eyre::Result<Self>
    where
        Net: FullNetwork + Clone + 'static,
    {
        let host: IpAddr = args
            .share_blocks_host
            .parse()
            .map_err(|e| eyre::eyre!("invalid --share-blocks-host: {e}"))?;
        let bind: SocketAddr = (host, args.share_blocks_port).into();

        let srv = Server::new(bind, &args.archive_dir).with_limits(512, 50);
        let _server = tokio::spawn(async move {
            if let Err(e) = srv.run().await {
                warn!(error=%e, "hlfs: server exited");
            }
        });

        let _autodetect = spawn_autodetect(network, host, args.share_blocks_port, args.archive_dir.clone());

        info!(%bind, dir=%args.archive_dir.display(), "hlfs: enabled (reth peers)");
        Ok(Self { _server, _autodetect })
    }

    // #[allow(dead_code)]
    // pub(crate) async fn try_fetch_one(&self, block: u64) -> eyre::Result<Option<usize>> {
    //     self._backfiller.fetch_if_missing(block).await.map_err(|e| eyre::eyre!(e))
    // }
}

fn spawn_autodetect<Net>(
    network: Net,
    self_ip: IpAddr,
    hlfs_port: u16,
    archive_dir: PathBuf,
) -> JoinHandle<()>
where
    Net: FullNetwork + Clone + 'static,
{
    let client = Client::new(&archive_dir, Vec::new()).with_timeout(Duration::from_secs(5));
    let backfiller = Arc::new(tokio::sync::Mutex::new(Backfiller::new(client, &archive_dir)));
    let good: Arc<tokio::sync::Mutex<HashSet<PeerRecord>>> =
        Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    tokio::spawn({
        let backfiller = backfiller.clone();
        async move {
            loop {
                let mut bf = backfiller.lock().await;
                warn!("hlfs: backfiller started");
                if bf.client.max_block < bf.max_block_seen {
                    let block = bf.client.max_block + 1;
                    let _ = bf.fetch_if_missing(block).await;
                }

                sleep(Duration::from_secs(1)).await;
            }
        }
    });

    tokio::spawn({
        let backfiller = backfiller.clone();
        async move {
            let mut events = network.event_listener();
            loop {
                let mut bf = backfiller.lock().await;
                match events.next().await {
                    Some(NetworkEvent::ActivePeerSession { info, .. }) => {
                        let ip = info.remote_addr.ip();
                        if ip.is_unspecified() {
                            debug!(%ip, "hlfs: skip unspecified");
                            continue;
                        }
                        if ip == self_ip {
                            debug!(%ip, "hlfs: skip self");
                            continue;
                        }
                        let addr = SocketAddr::new(info.remote_addr.ip(), hlfs_port);
                        let max_block = probe_hlfs(addr).await;
                        if max_block != 0 {
                            let mut g = good.lock().await;
                            if g.insert(PeerRecord { addr, max_block }) {
                                let v: Vec<_> = g.iter().copied().collect();
                                bf.set_peers(v.clone());
                                info!(%addr, %max_block, total=v.len(), "hlfs: peer added");
                            }
                        } else {
                            debug!(%addr, "hlfs: peer has no HLFS");
                        }
                    }
                    Some(_) => {}
                    None => {
                        warn!("hlfs: network event stream ended");
                        break;
                    }
                }
            }
        }
    })
}

pub(crate) async fn probe_hlfs(addr: SocketAddr) -> u64 {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    let fut = async {
        let mut s = TcpStream::connect(addr).await.ok()?;

        // send [OP][8 zero bytes]
        let mut msg = [0u8; 9];
        msg[0] = OP_REQ_MAX_BLOCK;
        s.write_all(&msg).await.ok()?;

        // read 1-byte opcode
        let mut op = [0u8; 1];
        s.read_exact(&mut op).await.ok()?;
        if op[0] != OP_RES_MAX_BLOCK {
            return None;
        }

        // read 8-byte little-endian block number
        let mut blk = [0u8; 8];
        s.read_exact(&mut blk).await.ok()?;
        Some(u64::from_le_bytes(blk))
    };

    match timeout(Duration::from_secs(2), fut).await {
        Ok(Some(n)) => n,
        _ => 0,
    }
}
