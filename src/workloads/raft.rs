use crate::common::NodeCtx;
use crate::messages::{
    Cas, CasOk, ErrorCode, Message, Read, ReadOk, RequestBody, ResponseBody, Write, WriteOk,
};
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug, Clone, Default)]
struct LinKvState {
    map: HashMap<usize, usize>,
}

// If this was a serious code base, this layer of the app wouldn't know about the wire request/response DTOs.
// But I'm not being too precious about it today...
impl LinKvState {
    /// Apply a request to the state machine
    ///
    /// If the request is relevant to the state machine, process it and return the appropriate response
    /// based on the lin-kv workload.
    ///
    /// If it's an unexpected request type, return the request in the Err.
    ///
    /// TODO: The set of valid request bodies for the state machine should be checked at compile time
    /// by having a more specific enum that covers these
    pub fn apply(
        &mut self,
        request_body: RequestBody,
    ) -> anyhow::Result<ResponseBody, RequestBody> {
        let resp = match request_body {
            RequestBody::Read(req) => self.read(req),
            RequestBody::Write(req) => self.write(req),
            RequestBody::Cas(req) => self.cas(req),
            body => {
                return Err(body);
            }
        };
        Ok(resp)
    }

    fn read(&self, Read { msg_id, key }: Read) -> ResponseBody {
        self.map
            .get(&key)
            .map(|v| {
                ResponseBody::ReadOk(ReadOk {
                    value: *v,
                    in_reply_to: msg_id,
                })
            })
            .unwrap_or(ResponseBody::Error {
                in_reply_to: msg_id,
                code: ErrorCode::KeyDoesNotExist,
                text: None,
            })
    }

    fn write(&mut self, Write { msg_id, key, value }: Write) -> ResponseBody {
        self.map.insert(key, value);

        ResponseBody::WriteOk(WriteOk {
            in_reply_to: msg_id,
        })
    }

    fn cas(
        &mut self,
        Cas {
            msg_id,
            key,
            from,
            to,
        }: Cas,
    ) -> ResponseBody {
        let Some(old_val) = self.map.get_mut(&key) else {
            return ResponseBody::Error {
                in_reply_to: msg_id,
                code: ErrorCode::KeyDoesNotExist,
                text: Some(format!("Key not found: {}", key)),
            };
        };

        if old_val != &from {
            return ResponseBody::Error {
                in_reply_to: msg_id,
                code: ErrorCode::PreconditionFailed,
                text: Some(format!(
                    "Current value didn't match the expected value. From: {:?}, actual current: {:?}",
                    from, old_val,
                )),
            };
        }

        *old_val = to;

        ResponseBody::CasOk(CasOk {
            in_reply_to: msg_id,
        })
    }
}

pub async fn raft(mut node_ctx: NodeCtx) -> anyhow::Result<()> {
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
