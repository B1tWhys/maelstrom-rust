use crate::messages::Message;

pub struct NodeCtx {
    pub node_id: String,

    pub nodes: Vec<String>,

    /// Receiver for inbound messages
    pub msgs_in: tokio::sync::broadcast::Receiver<Message>,

    /// Sender for outbound messages
    pub msgs_out: tokio::sync::mpsc::Sender<Message>,
}
