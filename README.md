# RAFT Consensus Algorithm - Go Implementation

A minimal implementation of the **RAFT consensus algorithm**, written in pure **Golang**, designed for building distributed, fault-tolerant systems with leader election, log replication, and state machine consistency.

---

## 🔧 What is RAFT?

[RAFT](https://raft.github.io/) is a consensus algorithm for managing a replicated log across a cluster of nodes, ensuring:

* **Leader Election**
* **Log Replication**
* **Fault Tolerance**
* **Strong Consistency**

RAFT simplifies building reliable distributed systems like databases, key-value stores, or configuration management tools.

---

## 🚀 Features

✅ Written in idiomatic Go, easy to integrate and extend
✅ Leader election with automatic failover
✅ Log replication and commit confirmation
✅ Cluster membership management
✅ Message-driven architecture suitable for simulations and real-world deployments
✅ Modular design for easy testing and embedding


---

## 📚 RAFT Roles

* **Leader**: Handles client requests, replicates logs to followers.
* **Follower**: Passive, responds to leader or candidates.
* **Candidate**: Starts election when no leader is detected.

---

## ⚙️ Communication Flow

* Nodes exchange messages: `AppendEntries`, `RequestVote`, and responses.
* Log entries are replicated to a quorum of nodes.
* Only committed entries are applied to the state machine.
* Leader changes handled seamlessly during failures.

---

## 🛠 Development Roadmap

* [x] Basic RAFT consensus with leader election and log replication
* [x] Persistent storage support for log and state recovery
* [x] Dynamic cluster membership changes
* [x] Detailed metrics and observability hooks
* [ ] Snapshotting for large state machines

---

## 🔒 Reliability Focus

* Failover tolerant: Leader re-election during node failures
* Strong consistency guarantees before responding to clients
* Modular transport layer for real or simulated environments

---

## 🤝 Contributing

Contributions, bug reports, and feature suggestions are welcome. Feel free to open an issue or pull request.

---

## 📄 License

MIT License — Open source, free for commercial and personal use.

---

## 💡 Inspiration

Inspired by the original RAFT paper and real-world distributed systems like etcd, Consul, and HashiCorp's Raft libraries.

---

## 🔗 Resources

* [The RAFT Paper (Diego Ongaro, 2014)](https://raft.github.io/raft.pdf)
* [RAFT Visualization](https://thesecretlivesofdata.com/raft/)

---
