# raftkv

![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Raft](https://img.shields.io/badge/Consensus-Raft-CC2927?style=for-the-badge&logo=apache&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-In%20Development-yellow?style=for-the-badge)
![Build](https://img.shields.io/badge/Build-Passing-brightgreen?style=for-the-badge&logo=githubactions&logoColor=white)

> A distributed key-value store built from scratch in Go, implementing the Raft consensus algorithm for leader election, log replication, and crash recovery — with chaos tests for network partitions and node failures.

---

## Overview

`raftkv` is not a wrapper around an existing consensus library. It's a from-scratch implementation of Raft, built to survive the failure modes that actually break naive distributed systems: crashed nodes, network partitions, and split-brain scenarios.

The project is deliberately staged so that correctness is proven incrementally — durability first, then consensus, then fault tolerance under chaos — rather than bolting everything together and hoping it holds.

---

## Tech Stack

![Go](https://img.shields.io/badge/Go-00ADD8?style=flat-square&logo=go&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-4285F4?style=flat-square&logo=googlecloud&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![JSON](https://img.shields.io/badge/JSON-000000?style=flat-square&logo=json&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white)

---

## Architecture

The system is split into two independent layers:

- **`raft/`** — the consensus engine. Knows nothing about keys or values; it only replicates an opaque log of commands across nodes and guarantees they're applied in the same order everywhere.
- **`kv/`** — the state machine. Applies committed log entries to an in-memory map. Could be swapped for a queue, a lock service, or anything else without touching consensus code.

This separation mirrors how production systems like etcd and CockroachDB are structured.

```
raftkv/
├── cmd/raftkv/      entrypoint — starts a node from config
├── raft/            leader election, log replication, WAL, RPC transport
├── kv/              the key-value store and its state machine
├── client/          client SDK — leader discovery, retries
├── test/            cluster integration tests and chaos tests
└── config/          cluster topology
```

---

## Core Guarantees

| Guarantee | How it's achieved |
|---|---|
| No data loss on crash | Every write hits a write-ahead log with `fsync` before being acknowledged |
| No data loss on leader failure | Writes are only committed after replication to a majority of nodes |
| No split-brain | A partitioned minority cannot elect a leader or accept writes |
| Consistent reads | Reads are served through the Raft-elected leader |

---

## Roadmap

- [x] **Milestone 1** — Single-node store with WAL-backed durability and crash recovery
- [ ] **Milestone 2** — Multi-node cluster with leader election
- [ ] **Milestone 3** — Log replication across followers
- [ ] **Milestone 4** — Chaos testing: killed nodes, partitioned networks, split-brain prevention
- [ ] **Milestone 5** — Log compaction via snapshots, client SDK, optional SQL layer

---

## Getting Started

```bash
git clone https://github.com/yourusername/raftkv.git
cd raftkv
go run ./cmd/raftkv
```

Requires Go 1.22 or later.

---

## Why Raft

Distributed consensus is one of the few problems in systems engineering where "looks like it works" and "is actually correct" diverge sharply. Raft was designed as an understandable alternative to Paxos, but understandable doesn't mean easy to implement correctly — every naive attempt tends to break the same way: under network partitions, where split-brain silently corrupts data. `raftkv` exists to implement and test that failure mode directly, not just the happy path.

---

## License

MIT