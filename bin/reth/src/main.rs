#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

mod block_ingest;
mod call_forwarder;
mod serialized;
mod share_blocks;
mod spot_meta;
mod tx_forwarder;

use std::{collections::BTreeMap, sync::Arc};

use block_ingest::BlockIngest;
use call_forwarder::CallForwarderApiServer;
use clap::{Args, Parser};
use reth::cli::Cli;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_hyperliquid_types::PrecompilesCache;
use reth_node_ethereum::EthereumNode;
use share_blocks::ShareBlocksArgs;
use tokio::sync::Mutex;
use tracing::info;
use tx_forwarder::EthForwarderApiServer;

#[derive(Args, Debug, Clone)]
struct HyperliquidExtArgs {
    /// Upstream RPC URL to forward incoming transactions.
    #[arg(long, default_value = "https://rpc.hyperliquid.xyz/evm")]
    pub upstream_rpc_url: String,

    /// Forward eth_call and eth_estimateGas to the upstream RPC.
    #[arg(long)]
    pub forward_call: bool,

    /// Enable hl-node compliant mode.
    ///
    /// This option
    /// 1. filters out system transactions from block transaction list.
    /// 2. filters out logs that are not from the block's transactions.
    /// 3. filters out logs and transactions from subscription.
    #[arg(long, default_value = "false")]
    pub hl_node_compliant: bool,

    /// Enable hlfs to backfill archive blocks
    #[command(flatten)]
    pub hlfs: ShareBlocksArgs,
}

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    let precompiles_cache = PrecompilesCache::new(parking_lot::Mutex::new(BTreeMap::new()));
    let local_blocks_cache = Arc::new(Mutex::new(BTreeMap::new()));

    if let Err(err) = Cli::<EthereumChainSpecParser, HyperliquidExtArgs>::parse().run(
        |builder, ext_args| async move {
            if ext_args.hl_node_compliant {
                info!(target: "reth::cli", "hl-node compliant mode enabled");
                std::env::set_var("HL_NODE_COMPLIANT", "true");
            }

            let ingest_dir = builder.config().ingest_dir.clone().expect("ingest dir not set");
            let local_ingest_dir = builder.config().local_ingest_dir.clone();
            info!(target: "reth::cli", "Launching node");
            let handle = builder
                .node(EthereumNode::default())
                .add_precompiles_cache(precompiles_cache.clone())
                .extend_rpc_modules(move |ctx| {
                    let upstream_rpc_url = ext_args.upstream_rpc_url;
                    ctx.modules.replace_configured(
                        tx_forwarder::EthForwarderExt::new(upstream_rpc_url.clone()).into_rpc(),
                    )?;

                    if ext_args.forward_call {
                        ctx.modules.replace_configured(
                            call_forwarder::CallForwarderExt::new(upstream_rpc_url.clone())
                                .into_rpc(),
                        )?;
                    }

                    info!("Transaction forwarder extension enabled");
                    Ok(())
                })
                .launch()
                .await?;

            // start HLFS (serve + peer-backed backfill) using the node's network
            if ext_args.hlfs.share_blocks {
                let net = handle.node.network.clone(); // returns a FullNetwork (NetworkHandle under the hood)
                                                         // keep handle alive for the whole process
                let _hlfs =
                    share_blocks::ShareBlocks::start_with_network(&ext_args.hlfs, net).await?;
            }

            let ingest =
                BlockIngest { ingest_dir, local_ingest_dir, local_blocks_cache, precompiles_cache };
            ingest.run(handle.node).await.unwrap();
            handle.node_exit_future.await
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
