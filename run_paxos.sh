#!/bin/bash -e

cargo b
export WORKLOAD=paxos RUST_BACKTRACE=1
./maelstrom/maelstrom test -w lin-kv --bin target/debug/maelstrom-rust \
  --time-limit 10 --rate 10 --node-count 3 --concurrency 2n \
  --log-stderr
