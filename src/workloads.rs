pub mod echo;
pub mod paxos;

use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub enum Workload {
    Echo,
    Paxos,
}
