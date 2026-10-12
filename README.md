# Maelstrom

This isn't anything super special. Just messing around with maelstrom/distributed consensus algorithms/rust.

For now there's just the most naive/ineffcient conceivable paxos linearizable key-value implementation. 
I "think" it's correct though!

#### Setup
Download/unpack maelstrom into the current directory (this has been tested on [0.2.4](https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.4)):

```sh
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2
tar -xf maelstrom.tar.bz2
```

#### Running
```sh
./run_paxos.sh
```
