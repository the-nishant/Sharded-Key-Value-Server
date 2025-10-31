# 6.824 Lab 4: Sharded Key/Value Service (Spring 2022)

## 🧠 Overview
This project implements a **fault-tolerant, sharded key/value storage system** using the **Raft consensus algorithm** for replication and consistency.  
The system partitions its keyspace into **shards**, each managed by a **replica group**, and a replicated **Shard Controller** dynamically assigns shards to groups.  
The goal is to build a scalable, reconfigurable storage system capable of maintaining **strong consistency** even during failures or reconfiguration events.

---

## 🧩 Architecture

### 1. Shard Controller (`src/shardctrler/`)
- Manages a sequence of **configurations** mapping shards to replica groups.  
- Handles the following RPCs:
  - `Join`: Add new replica groups  
  - `Leave`: Remove replica groups  
  - `Move`: Manually reassign a shard to a different group  
  - `Query`: Fetch the latest or specific configuration  
- Uses **Raft** for replication and fault-tolerance.  
- Balances shards evenly while minimizing unnecessary data movement.

### 2. Sharded Key/Value Server (`src/shardkv/`)
- Each replica group serves Get/Put/Append requests for its assigned shards.  
- Uses **Raft** to replicate operations and maintain strong consistency.  
- Handles **shard migration** during configuration changes:
  - Transfers data via RPC between groups.  
  - Rejects requests for shards it no longer owns.  
- Guarantees:
  - **Linearizability** (operations appear in a single global order).  
  - **At-most-once** semantics for client requests.  
- Functions correctly under partial failures or network partitions.

---

## ⚙️ Key Features
- **Dynamic Shard Reconfiguration** — automatic reassignment of shards on joins/leaves.  
- **Fault-Tolerant Coordination** — Raft ensures consistent replication across all groups.  
- **Shard Migration** — seamless and consistent data transfer during reconfiguration.  
- **Duplicate Request Handling** — idempotence across unreliable network conditions.  
