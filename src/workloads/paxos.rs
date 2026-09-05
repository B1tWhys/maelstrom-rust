use crate::common::NodeCtx;
use crate::lin_kv::LinKvState;
use crate::messages::{ErrorCode, Message, ResponseBody};
use tracing::{info, warn};

pub async fn paxos(mut node_ctx: NodeCtx) -> anyhow::Result<()> {
    let mut state = LinKvState::default();

    while let Ok(Message {
        src: req_src, body, ..
    }) = node_ctx.req_in.recv().await
    {
        info!(?body, "Handling request");
        let resp = state.apply(body).unwrap_or_else(|req| {
            warn!(
                ?req,
                "Unexpected message body received for raft, ignoring it",
            );
            ResponseBody::Error {
                in_reply_to: req.msg_id(),
                code: ErrorCode::MalformedRequest,
                text: Some(format!(
                    "raft doesn't know how to deal with request: {:?}",
                    req
                )),
            }
        });
        info!(?resp, "Responding to request");
        node_ctx
            .rsp_out
            .send(Message {
                src: node_ctx.node_id.clone(),
                dest: req_src,
                body: resp,
            })
            .await?;
    }

    Ok(())
}
