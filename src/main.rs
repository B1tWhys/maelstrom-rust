#[macro_use]
extern crate macro_rules_attribute;

pub mod common;
pub mod lin_kv;
mod messages;
mod workloads;

use crate::common::NodeCtx;
use crate::messages::{Init, Message, MessageBody, RequestBody, ResponseBody};
use crate::workloads::Workload;
use crate::workloads::echo::echo;
use crate::workloads::paxos::PaxosNode;
use anyhow::{Context, anyhow, bail};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::{Level, debug, error, info, warn};

const WORKLOAD_ENV_VAR: &str = "WORKLOAD";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configure_logging()?;
    let workload = select_workload()?;
    info!(?workload, "Starting up");

    let stdin = connect_stdin();
    let stdout = connect_stdout();

    let node_ctx = init_node_ctx(stdin, stdout).await?;

    info!("Initialized node context");

    match workload {
        Workload::Echo => echo(node_ctx).await?,
        Workload::Paxos => paxos(node_ctx).await?,
    }

    Ok(())
}

async fn paxos(node_ctx: NodeCtx) -> anyhow::Result<()> {
    let paxos = Arc::new(PaxosNode::new(node_ctx));
    paxos.run().await
}

fn configure_logging() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .without_time()
        .with_target(false)
        .with_writer(io::stderr)
        .with_max_level(Level::DEBUG)
        .try_init()
        .map_err(|e| anyhow!(e))
}

fn select_workload() -> anyhow::Result<Workload> {
    let val = std::env::vars()
        .find(|(k, _)| k == WORKLOAD_ENV_VAR)
        .map(|(_, v)| v)
        .unwrap_or_else(|| {
            warn!(
                "No workload picked up from the {} env var, defaulting to: echo",
                WORKLOAD_ENV_VAR
            );
            "echo".into()
        });

    if val == "echo" {
        Ok(Workload::Echo)
    } else if val == "paxos" {
        Ok(Workload::Paxos)
    } else {
        bail!("Unexpected workload: {}", val);
    }
}

fn connect_stdin() -> Receiver<Message> {
    let (req_tx, req_rx) = tokio::sync::broadcast::channel::<Message>(100);

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    tokio::task::spawn(async move {
        while let Ok(Some(line)) = stdin.next_line().await {
            let msg = match serde_json::from_str::<Message>(&line) {
                Ok(msg) => msg,
                Err(e) => {
                    error!(%e, %line, "Failed to deserialize line");
                    continue;
                }
            };
            req_tx
                .send(msg)
                .expect("Message received without any consumers wired up to handle it!");
        }
    });

    req_rx
}

fn connect_stdout() -> Sender<Message> {
    let (req_out_tx, mut req_out_rx) = tokio::sync::mpsc::channel::<Message>(128);

    tokio::spawn(async move {
        while let Some(msg) = req_out_rx.recv().await {
            let message = serde_json::to_string(&msg)
                .with_context(|| format!("Failed to serialize message: {:?}", msg))
                .unwrap();
            println!("{}", message);
            debug!(stdout_msg = message);
        }
        info!("Stdout closed");
    });

    req_out_tx
}

async fn init_node_ctx(
    mut msgs_in: Receiver<Message>,
    msgs_out: Sender<Message>,
) -> anyhow::Result<NodeCtx> {
    let Ok(Message {
        src: init_src,
        body:
            MessageBody::Request {
                msg_id,
                req_body: RequestBody::Init(Init { node_id, node_ids }),
            },
        ..
    }) = msgs_in.recv().await
    else {
        bail!("Failed to receive an init message!")
    };

    let node_ctx = NodeCtx {
        node_id,
        nodes: node_ids,
        msgs_in,
        msgs_out,
    };

    node_ctx
        .msgs_out
        .send(Message {
            src: node_ctx.node_id.clone(),
            dest: init_src,
            body: MessageBody::Response {
                in_reply_to: msg_id,
                rsp_body: ResponseBody::InitOk,
            },
        })
        .await?;
    Ok(node_ctx)
}
