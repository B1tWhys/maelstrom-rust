use crate::messages::{Message, MessageBody, RequestBody, ResponseBody};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::warn;

#[derive(Debug)]
pub struct NodeCtx {
    /// My node id
    pub node_id: String,

    /// List of node ids in the cluster (including self)
    pub nodes: Vec<String>,

    /// Receiver for inbound messages
    pub msgs_in: Mutex<UnboundedReceiver<Message>>,

    /// Sender for outbound messages
    pub msgs_out: UnboundedSender<Message>,
}

impl NodeCtx {
    /// All the node ids that aren't me
    pub fn other_nodes(&self) -> impl Iterator<Item = &String> {
        self.nodes.iter().filter(|&n| n != &self.node_id)
    }

    pub fn min_quorum_size(&self) -> usize {
        let l = self.nodes.len();
        l / 2 + 1
    }
}

impl NodeCtx {
    pub async fn next_request(&self) -> Option<Message> {
        while let Some(msg) = self.msgs_in.lock().await.recv().await {
            match &msg.body {
                MessageBody::Request { .. } => {
                    return Some(msg);
                }
                MessageBody::Response { .. } => {
                    todo!("Handle responses while digging for a request")
                }
            }
        }
        None
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
        if let Err(e) = self.msgs_out.send(msg) {
            warn!(
                ?request,
                %e,
                "Tried to output to stdout but the sender was closed"
            )
        }
    }

    pub async fn send_request(
        &self,
        _dest: String,
        _request_body: RequestBody,
    ) -> anyhow::Result<ResponseBody> {
        todo!()
    }
}
