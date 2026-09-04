#!/bin/bash -e

cargo b
./maelstrom/maelstrom test -w echo --bin target/debug/maelstrom-rust --nodes n1 --time-limit 10 --log-stderr