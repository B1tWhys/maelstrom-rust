#[macro_use]
extern crate macro_rules_attribute;

pub mod common;
mod echo;
mod messages;

use crate::common::NodeCtx;
use crate::echo::echo;
use crate::messages::{Init, InitOk, Message, RequestBody, ResponseBody};
use anyhow::{Context, anyhow, bail};
use std::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configure_logging()?;
    info!("Starting up");

    let req_in = connect_stdin();
    let (req_out, rsp_out) = connect_stdout();

    let node_ctx = init_node_ctx(req_in, req_out, rsp_out).await?;

    info!("Initialized node context");

    echo(node_ctx).await?;

    Ok(())
}

fn configure_logging() -> anyhow::Result<()> {
    tracing_subscriber::fmt::fmt()
        .without_time()
        .with_target(false)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|e| anyhow!(e))
}

fn connect_stdin() -> Receiver<Message<RequestBody>> {
    let (req_tx, req_rx) = tokio::sync::broadcast::channel::<Message<RequestBody>>(10);

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();

    tokio::task::spawn(async move {
        while let Ok(Some(line)) = stdin.next_line().await {
            let msg = match serde_json::from_str::<Message<RequestBody>>(&line) {
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

/// # Returns
/// `(req_out_tx, rsp_out_tx)`
fn connect_stdout() -> (Sender<Message<RequestBody>>, Sender<Message<ResponseBody>>) {
    let (req_out_tx, mut req_out_rx) = tokio::sync::mpsc::channel::<Message<RequestBody>>(128);
    let (rsp_out_tx, mut rsp_out_rx) = tokio::sync::mpsc::channel::<Message<ResponseBody>>(128);

    tokio::spawn(async move {
        loop {
            // Erase type of message body
            let message = tokio::select! {
                Some(msg) = req_out_rx.recv() => {
                    serde_json::to_string(&msg).with_context(|| format!("Failed to serialize message: {:?}", msg)).unwrap()
                }
                Some(msg) = rsp_out_rx.recv() => {
                    serde_json::to_string(&msg).with_context(|| format!("Failed to deserialize message: {:?}", msg)).unwrap()
                }
                else => {
                    break
                }
            };

            println!("{}", message);
        }
    });

    (req_out_tx, rsp_out_tx)
}

async fn init_node_ctx(
    mut req_in: Receiver<Message<RequestBody>>,
    req_out: Sender<Message<RequestBody>>,
    rsp_out: Sender<Message<ResponseBody>>,
) -> anyhow::Result<NodeCtx> {
    let Ok(Message {
        src: init_src,
        body:
            RequestBody::Init(Init {
                node_id,
                msg_id: init_msg_id,
                ..
            }),
        ..
    }) = req_in.recv().await
    else {
        bail!("Failed to receive an init message!")
    };

    let node_ctx = NodeCtx {
        node_id,
        req_in,
        req_out,
        rsp_out,
    };

    node_ctx
        .rsp_out
        .send(Message {
            src: node_ctx.node_id.clone(),
            dest: init_src,
            body: ResponseBody::InitOk(InitOk {
                in_reply_to: init_msg_id,
            }),
        })
        .await?;
    Ok(node_ctx)
}
