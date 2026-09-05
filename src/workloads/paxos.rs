use crate::common::NodeCtx;
use crate::lin_kv::LinKvState;
use crate::messages::{ErrorCode, Message, MessageBody, ResponseBody};
use tracing::{info, warn};

pub async fn paxos(mut node_ctx: NodeCtx) -> anyhow::Result<()> {
    let mut state = LinKvState::default();

    while let Ok(Message {
        src: req_src,
        body: MessageBody::Request {
            msg_id,
            req_body: body,
        },
        ..
    }) = node_ctx.msgs_in.recv().await
    {
        info!(?body, msg_id, "Handling request");
        let rsp_body = state.apply(body).unwrap_or_else(|req| {
            warn!(
                ?req,
                "Unexpected message body received for raft, ignoring it",
            );
            ResponseBody::Error {
                code: ErrorCode::MalformedRequest,
                text: Some(format!(
                    "raft doesn't know how to deal with request: {:?}",
                    req
                )),
            }
        });

        let resp = Message {
            src: node_ctx.node_id.clone(),
            dest: req_src,
            body: MessageBody::Response {
                in_reply_to: msg_id,
                rsp_body,
            },
        };
        info!(?resp, "Responding to request");
        node_ctx.msgs_out.send(resp).await?;
    }

    Ok(())
}
