use crate::messages::{Message, MessageBody, RequestBody, ResponseBody};
use crate::node_ctx::NodeCtx;
use anyhow::bail;
use std::sync::Arc;
use tracing::{debug, info};

pub async fn echo(node_ctx: Arc<NodeCtx>) -> anyhow::Result<()> {
    info!("Starting echo");
    while let Some(req) = &node_ctx.recv_request().await {
        debug!(?req, "Echo received request");
        let Message {
            src,
            dest,
            body:
                MessageBody::Request {
                    msg_id,
                    req_body: RequestBody::Echo { echo },
                },
        } = req
        else {
            bail!("Received unexpected request for echo: {:?}", req);
        };

        node_ctx
            .send_response(req, ResponseBody::EchoOk { echo: echo.clone() })
            .await;
    }

    Ok(())
}
