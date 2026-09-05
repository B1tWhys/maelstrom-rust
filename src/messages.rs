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
pub struct Message<T>
where
    T: Serialize,
{
    pub src: String,
    pub dest: String,
    pub body: T,
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum RequestBody {
    Init(Init),
    Echo(Echo),
    Read(Read),
    Write(Write),
    Cas(Cas),
}

impl RequestBody {
    pub fn msg_id(&self) -> usize {
        match self {
            RequestBody::Init(Init { msg_id, .. })
            | RequestBody::Echo(Echo { msg_id, .. })
            | RequestBody::Read(Read { msg_id, .. })
            | RequestBody::Write(Write { msg_id, .. })
            | RequestBody::Cas(Cas { msg_id, .. }) => *msg_id,
        }
    }
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum ResponseBody {
    InitOk(InitOk),
    EchoOk(EchoOk),
    ReadOk(ReadOk),
    WriteOk(WriteOk),
    CasOk(CasOk),
    Error {
        in_reply_to: usize,
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
    pub msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[apply(message_derives)]
pub struct InitOk {
    pub in_reply_to: usize,
}

// ===== Echo =====

#[apply(message_derives)]
pub struct Echo {
    pub msg_id: usize,
    pub echo: String,
}

#[apply(message_derives)]
pub struct EchoOk {
    pub echo: String,
    pub in_reply_to: usize,
}

// ===== Lin-kv =====

#[apply(message_derives)]
pub struct Read {
    pub msg_id: usize,
    pub key: usize,
}

#[apply(message_derives)]
pub struct ReadOk {
    pub value: usize,
    pub in_reply_to: usize,
}

#[apply(message_derives)]
pub struct Write {
    pub msg_id: usize,
    pub key: usize,
    pub value: usize,
}

#[apply(message_derives)]
pub struct WriteOk {
    pub in_reply_to: usize,
}

#[apply(message_derives)]
pub struct Cas {
    pub msg_id: usize,
    pub key: usize,
    pub from: usize,
    pub to: usize,
}

#[apply(message_derives)]
pub struct CasOk {
    pub in_reply_to: usize,
}
