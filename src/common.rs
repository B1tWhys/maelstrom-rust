use crate::messages::{Message, RequestBody, ResponseBody};

pub struct NodeCtx {
    pub node_id: String,

    /// Receiver for incoming request messages
    pub req_in: tokio::sync::broadcast::Receiver<Message<RequestBody>>,

    /// Sender for outbound requests (i.e. to other nodes)
    pub req_out: tokio::sync::mpsc::Sender<Message<RequestBody>>,

    /// Sender for outbound responses (responses to requests)
    pub rsp_out: tokio::sync::mpsc::Sender<Message<ResponseBody>>,
}
