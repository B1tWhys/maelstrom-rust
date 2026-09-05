use crate::common::NodeCtx;
use crate::messages::{Echo, EchoOk, Message, RequestBody, ResponseBody};
use anyhow::bail;
use tracing::info;

pub async fn echo(mut node_ctx: NodeCtx) -> anyhow::Result<()> {
    info!("Starting echo");
    while let Ok(req) = &node_ctx.req_in.recv().await {
        let Message {
            src,
            dest,
            body: RequestBody::Echo(Echo { msg_id, echo }),
        } = req
        else {
            bail!("Received unexpected request for echo: {:?}", req);
        };

        let resp = Message {
            src: dest.clone(),
            dest: src.clone(),
            body: ResponseBody::EchoOk(EchoOk {
                echo: echo.clone(),
                in_reply_to: *msg_id,
            }),
        };

        node_ctx.rsp_out.send(resp).await?;
    }

    Ok(())
}
