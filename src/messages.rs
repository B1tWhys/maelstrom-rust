#![allow(dead_code)] // FIXME

use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

macro_rules! message_derives {
    ($s: item) => {
        #[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        $s
    };
}

#[apply(message_derives)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

#[apply(message_derives)]
#[serde(untagged)]
pub enum MessageBody {
    Request {
        msg_id: usize,
        #[serde(flatten)]
        req_body: RequestBody,
    },
    Response {
        in_reply_to: usize,
        #[serde(flatten)]
        rsp_body: ResponseBody,
    },
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum RequestBody {
    Init(Init),
    Echo {
        echo: String,
    },
    #[serde(untagged)]
    LinKvOp(LinKvOp),
    #[serde(untagged)]
    Paxos(PaxosReq),
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum LinKvOp {
    Read { key: usize },
    Write { key: usize, value: usize },
    Cas { key: usize, from: usize, to: usize },
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum PaxosReq {
    Prepare {
        seq_num: PaxosSeqN,
        slot_idx: usize,
    },
    Propose {
        seq_num: PaxosSeqN,
        slot_idx: usize,
        value: LinKvOp,
    },
    Decided {
        slot_idx: PaxosSeqN,
        value: LinKvOp,
    },
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum ResponseBody {
    InitOk,
    EchoOk {
        echo: String,
    },
    ReadOk {
        value: usize,
    },
    WriteOk,
    CasOk,
    Error {
        code: ErrorCode,
        text: Option<String>,
    },
    #[serde(untagged)]
    Paxos(PaxosRsp),
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum PaxosRsp {
    Promise { existing_proposal: Option<Proposal> },
    AcceptOk,
    Nack(PaxosNack),
}

#[apply(message_derives)]
pub enum PaxosNack {
    NTooLow(PaxosSeqN),
    AlreadyDecided(LinKvOp),
}

#[apply(message_derives)]
pub struct Proposal {
    pub seq_num: PaxosSeqN,
    pub value: LinKvOp,
}

#[apply(message_derives)]
#[derive(PartialOrd, Ord)]
pub struct PaxosSeqN {
    pub counter: usize,
    pub node_id: String,
}

impl Display for PaxosSeqN {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.counter, self.node_id)
    }
}

// ===== Error =====

/// Error codes copied from: https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors
#[apply(message_derives)]
#[derive(TryFromPrimitive, IntoPrimitive)]
#[serde(try_from = "u16", into = "u16")]
#[repr(u16)]
pub enum ErrorCode {
    /// The requested operation could not be completed within a timeout.
    Timeout = 0,

    /// A client sent an RPC request to a node which does not exist.
    NodeNotFound = 1,

    /// The requested operation is not supported by the current implementation.
    NotSupported = 10,

    /// The operation definitely cannot be performed at this time, perhaps
    /// because the server is read-only, uninitialized, or believes its peers
    /// to be down. Not for indeterminate cases.
    TemporarilyUnavailable = 11,

    /// The client's request did not conform to the server's expectations and
    /// could not possibly have been processed.
    MalformedRequest = 12,

    /// A general, indefinite error. Safe catch-all for uncategorized problems.
    Crash = 13,

    /// A general, definite error: the requested operation has not taken place.
    Abort = 14,

    /// An operation was requested on a key which does not exist.
    KeyDoesNotExist = 20,

    /// Creation was requested for a key which already exists, and the server
    /// will not overwrite it.
    KeyAlreadyExists = 21,

    /// The conditions the requested operation expected were not met.
    PreconditionFailed = 22,

    /// The transaction was aborted because of a conflict with another
    /// transaction.
    TxnConflict = 30,
}

// ===== Init =====

#[apply(message_derives)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

// ===== Lin-kv =====

#[apply(message_derives)]
pub struct Read {
    pub key: usize,
}

#[apply(message_derives)]
pub struct Write {
    pub key: usize,
    pub value: usize,
}

#[apply(message_derives)]
pub struct Cas {
    pub key: usize,
    pub from: usize,
    pub to: usize,
}
