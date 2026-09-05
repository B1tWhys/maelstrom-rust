use crate::messages::{Message, ResponseBody};

pub struct NodeCtx {
    /// My node id
    pub node_id: String,

    /// List of node ids in the cluster (including self)
    pub nodes: Vec<String>,

    /// Receiver for inbound messages
    pub msgs_in: tokio::sync::broadcast::Receiver<Message>,

    /// Sender for outbound messages
    pub msgs_out: tokio::sync::mpsc::Sender<Message>,
}

impl NodeCtx {
    pub async fn next_request(&self) -> Option<Message> {
        todo!()
    }

    pub async fn send_response(&self, _request: &Message, _resp_body: ResponseBody) {
        todo!()
    }
}
