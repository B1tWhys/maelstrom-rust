use serde::{Deserialize, Serialize};

macro_rules! std_serde {
    ($s: item) => {
        #[derive(Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        $s
    };
}

#[apply(std_serde)]
pub struct Message<T> {
    pub src: String,
    pub dest: String,
    pub body: T,
}

#[apply(std_serde)]
#[serde(tag = "type")]
pub enum RequestBody {
    Init(Init),
    Echo(Echo)
}


#[apply(std_serde)]
#[serde(tag = "type")]
pub enum ResponseBody {
    InitOk(InitOk),
    EchoOk(EchoOk)
}


#[apply(std_serde)]
pub struct Init {
    pub msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[apply(std_serde)]
pub struct InitOk {
    pub in_reply_to: usize,
}

#[apply(std_serde)]
pub struct Echo {
    pub msg_id: usize,
    pub echo: String,
}

#[apply(std_serde)]
pub struct EchoOk {
    pub echo: String,
    pub in_reply_to: usize,
}
