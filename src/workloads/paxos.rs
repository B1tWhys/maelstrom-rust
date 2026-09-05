#![allow(dead_code)] // FIXME

use crate::common::NodeCtx;
use crate::lin_kv::LinKvState;
use crate::messages::{ErrorCode, LinKvOp, Message, MessageBody, RequestBody, ResponseBody};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use tracing::info;

/// Represents a node in a cluster of hosts running paxos. Encapsulates all that logic & state.
pub struct PaxosNode {
    node_ctx: NodeCtx,
    /// The node id of the current leader. See [`NodeCtx::nodes`]
    cur_leader: RwLock<String>,
    /// The current sequence number for this paxos node. All paxos rounds/slots use this same counter
    paxos_seq_no: AtomicUsize,
    /// The titular key-value store. Once operations are decided in the log, they are applied (in order)
    /// to this snapshot.
    snapshot: RwLock<LinKvState>,
    /// Index of the next slot in the log to apply to the snapshot (once it's ready). May be equal
    /// to `log.len()` if all slots in the log have been applied already
    next_slot_to_apply: AtomicUsize,
    /// Log of the paxos slots. Each slot represents one round of paxos, corresponding to one atomic
    /// operation on the snapshot
    log: RwLock<Vec<PaxosSlot>>,
}

/// State machine for 1 round of paxos
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum PaxosSlot {
    New,
    Promised { seq_no: usize },
    Accepted { seq_no: usize, value: LinKvOp },
    Decided { value: LinKvOp },
}

impl PaxosNode {
    pub fn new(node_ctx: NodeCtx) -> Self {
        let cur_leader = node_ctx
            .nodes
            .iter()
            .min()
            .cloned()
            .expect("NodeCtx has no nodes! It's gonna be hard to do paxos without them!");
        // assert!(
        //     node_ctx.nodes.len() > 1,
        //     "There aren't enough nodes to do paxos!"
        // );

        info!("Assuming {cur_leader} is leader node (TODO: Do this through an election)");

        Self {
            node_ctx,
            cur_leader: RwLock::new(cur_leader),
            paxos_seq_no: Default::default(),
            snapshot: Default::default(),
            next_slot_to_apply: Default::default(),
            log: RwLock::new(vec![]),
        }
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        while let Some(message) = self.node_ctx.next_request().await {
            let self_arc = Arc::clone(&self);
            tokio::spawn(async move {
                let resp = self_arc.handle_request(&message).await;
                self_arc.node_ctx.send_response(&message, resp).await;
            });
        }

        Ok(())
    }

    async fn handle_request(&self, message: &Message) -> ResponseBody {
        let Message {
            body:
                MessageBody::Request {
                    msg_id: _,
                    req_body,
                },
            ..
        } = message
        else {
            panic!(
                "This function is only supposed to handle requests! Got: {:?}",
                message
            );
        };

        match req_body {
            RequestBody::Init(_) | RequestBody::Echo { .. } => ResponseBody::Error {
                code: ErrorCode::NotSupported,
                text: Some(format!("paxos doesn't support request: {:?}", message)),
            },
            RequestBody::LinKvOp(op) => self.handle_linkv_op(op).await,
        }
    }

    async fn handle_linkv_op(&self, op: &LinKvOp) -> ResponseBody {
        // TODO: do paxos shit
        let mut snapshot = self.snapshot.write().unwrap();
        snapshot.apply(op.clone())
    }
}
