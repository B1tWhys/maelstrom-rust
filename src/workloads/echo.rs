use crate::common::NodeCtx;
use crate::messages::{Message, MessageBody, RequestBody, ResponseBody};
use anyhow::bail;
use tracing::info;

pub async fn echo(node_ctx: NodeCtx) -> anyhow::Result<()> {
    info!("Starting echo");
    while let Some(req) = &node_ctx.next_request().await {
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

        let resp = Message {
            src: dest.clone(),
            dest: src.clone(),
            body: MessageBody::Response {
                in_reply_to: *msg_id,
                rsp_body: ResponseBody::EchoOk { echo: echo.clone() },
            },
        };

        node_ctx.msgs_out.send(resp)?;
    }

    Ok(())
}
