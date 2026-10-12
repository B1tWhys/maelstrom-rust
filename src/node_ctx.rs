use crate::messages::{Message, MessageBody, RequestBody, ResponseBody};
use anyhow::Context;
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Mutex, oneshot};
use tracing::warn;

#[derive(Debug)]
pub struct NodeCtx {
    /// My node id
    pub node_id: String,

    /// List of node ids in the cluster (including self)
    pub nodes: Vec<String>,

    /// Sender for outbound messages
    msgs_out: UnboundedSender<Message>,

    /// This sender injects messages into the inbound message receive flow.
    ///
    /// When we try to send a message to the current node id, instead of writing it to stdout & letting
    /// maelstrom do the round-tripping (possibly with delay & counting against efficiency metrics),
    /// we just send the message through here & skip the maelstrom round-trip.
    msgs_in_tx: UnboundedSender<Message>,

    /// Used for calculating unique (to this node) message ids
    msg_id: AtomicUsize,

    /// Oneshot channels corresponding to outbound requests that we're waiting to hear back about
    pending_responses: Mutex<HashMap<usize, oneshot::Sender<ResponseBody>>>,

    /// Queue of parsed requests waiting to be handled. To get queue semantics (when consuming from
    /// multiple tasks) this is a `Mutex<mpsc::UnboundedReceiver>`, rather than using a broadcast.
    req_queue_rx: Mutex<UnboundedReceiver<Message>>,
}

impl NodeCtx {
    pub fn new(
        node_id: String,
        nodes: Vec<String>,
        msgs_in: UnboundedReceiver<Message>,
        msgs_in_tx: UnboundedSender<Message>,
        msgs_out: UnboundedSender<Message>,
    ) -> Arc<Self> {
        let (req_queue_tx, req_queue_rx) = unbounded_channel();
        let ret = Arc::new(Self {
            node_id,
            nodes,
            msgs_out,
            msgs_in_tx,
            msg_id: Default::default(),
            pending_responses: Default::default(),
            req_queue_rx: Mutex::new(req_queue_rx),
        });

        let movable = ret.clone();
        tokio::spawn(async move {
            movable.handle_inbound_msgs(msgs_in, req_queue_tx).await;
        });

        ret
    }

    async fn handle_inbound_msgs(
        &self,
        mut msgs_in: UnboundedReceiver<Message>,
        msg_queue_tx: UnboundedSender<Message>,
    ) {
        while let Some(m) = msgs_in.recv().await {
            match m.body {
                MessageBody::Request { .. } => {
                    msg_queue_tx.send(m).unwrap();
                }
                MessageBody::Response {
                    in_reply_to,
                    rsp_body,
                } => {
                    let mut pending_reqs = self.pending_responses.lock().await;
                    if let Some(one_shot) = pending_reqs.remove(&in_reply_to)
                        && let Err(e) = one_shot.send(rsp_body)
                    {
                        debug!("Received unsolicited response. Ignoring it: {e:?}");
                    }
                }
            }
        }
    }

    /// All the node ids that aren't me
    pub fn other_nodes(&self) -> impl Iterator<Item = &String> {
        self.nodes.iter().filter(|&n| n != &self.node_id)
    }

    pub fn min_quorum_size(&self) -> usize {
        let l = self.nodes.len();
        l / 2 + 1
    }

    pub async fn recv_request(&self) -> Option<Message> {
        self.req_queue_rx.lock().await.recv().await
    }

    pub async fn send_response(&self, request: &Message, rsp_body: ResponseBody) {
        let Message {
            body: MessageBody::Request { msg_id, .. },
            ..
        } = request
        else {
            panic!(
                "Tried to respond to a response. Doesn't work! {:?}",
                request
            );
        };

        let msg = Message {
            src: self.node_id.clone(),
            dest: request.src.clone(),
            body: MessageBody::Response {
                in_reply_to: *msg_id,
                rsp_body,
            },
        };

        let result = if msg.dest == self.node_id {
            // Skip the stdout/maelstrom/stdin round-trip, just put the message directly into the inbound mpsc channel
            self.msgs_in_tx.send(msg)
        } else {
            self.msgs_out.send(msg)
        };

        if let Err(e) = result {
            warn!(
                ?request,
                %e,
                "Tried to output a message but the sender was closed"
            )
        }
    }

    pub async fn send_request(
        &self,
        dest: String,
        request_body: RequestBody,
    ) -> anyhow::Result<ResponseBody> {
        let msg_id = self.msg_id.fetch_add(1, Ordering::SeqCst); // Should be Relaxed l8r

        let (oneshot_tx, oneshot_rx) = oneshot::channel();
        self.pending_responses
            .lock()
            .await
            .insert(msg_id, oneshot_tx);

        let msg = Message {
            src: self.node_id.clone(),
            dest: dest.clone(),
            body: MessageBody::Request {
                msg_id,
                req_body: request_body,
            },
        };

        if dest == self.node_id {
            // Skip the stdout/maelstrom/stdin round-trip, just put the message directly into the inbound mpsc channel
            self.msgs_in_tx.send(msg)?;
        } else {
            self.msgs_out.send(msg)?;
        }

        oneshot_rx
            .await
            .with_context(|| format!("Failed to await response for msg: {}", msg_id))
    }
}
