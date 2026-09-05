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
}

#[apply(message_derives)]
#[serde(tag = "type")]
pub enum ResponseBody {
    InitOk(InitOk),
    EchoOk(EchoOk),
}

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
