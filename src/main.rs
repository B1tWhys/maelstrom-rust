#[macro_use]
extern crate macro_rules_attribute;

mod messages;

use crate::messages::{EchoOk, InitOk, Message, RequestBody, ResponseBody};
use anyhow::{Context, anyhow};
use std::io;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::error;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    configure_logging()?;

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut my_node_id = String::default();

    while let Ok(Some(line)) = stdin.next_line().await {
        let msg = match serde_json::from_str::<Message<RequestBody>>(&line) {
            Ok(msg) => msg,
            Err(e) => {
                error!(%e, %line, "Failed to deserialize line");
                continue;
            }
        };

        let resp_body = match &msg.body {
            RequestBody::Init(init) => {
                my_node_id = init.node_id.clone();
                ResponseBody::InitOk(InitOk {
                    in_reply_to: init.msg_id,
                })
            }
            RequestBody::Echo(echo) => ResponseBody::EchoOk(EchoOk {
                echo: echo.echo.clone(),
                in_reply_to: echo.msg_id,
            }),
        };

        let resp = Message {
            src: my_node_id.clone(),
            dest: msg.src,
            body: resp_body,
        };

        println!(
            "{}",
            serde_json::to_string(&resp)
                .with_context(|| format!("Failed to serialize: {:?}", resp))?
        );
    }

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
