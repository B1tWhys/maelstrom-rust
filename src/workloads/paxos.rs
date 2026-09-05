#![allow(dead_code)] // FIXME

use crate::common::NodeCtx;
use crate::lin_kv::LinKvState;
use crate::messages::{ErrorCode, LinKvOp, Message, MessageBody, RequestBody, ResponseBody};
use std::fmt::Debug;
use std::hash::Hash;
use tracing::{info, warn};

struct PaxosState {
    cur_leader: String,
    snapshot: LinKvState,
    slots: Vec<PaxosSlot>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PaxosSlot {
    max_decided_slot: usize,
    slot_state: SlotState,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum SlotState {
    New,
    Promised { seq_no: usize },
    Accepted { seq_no: usize, value: LinKvOp },
    Decided { value: LinKvOp },
}

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

        let rsp_body = match body {
            RequestBody::LinKvOp(lin_kv_op) => state.apply(lin_kv_op),
            _ => {
                warn!(
                    ?body,
                    "Unexpected message body received for raft, ignoring it",
                );
                ResponseBody::Error {
                    code: ErrorCode::MalformedRequest,
                    text: Some(format!(
                        "paxos doesn't know how to deal with request: {:?}",
                        body
                    )),
                }
            }
        };

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
