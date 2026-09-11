#!/bin/bash -e

cargo b
export WORKLOAD=paxos
./maelstrom/maelstrom test -w lin-kv --bin target/debug/maelstrom-rust \
  --time-limit 10 --rate 10 --node-count 1 --concurrency 2n \
  --log-stderr
