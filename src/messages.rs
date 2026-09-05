use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

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
    Echo { echo: String },
    Read { key: usize },
    Write(Write),
    Cas(Cas),
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
