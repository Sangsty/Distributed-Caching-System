# Design notes

## Goal
Spread cache keys over several nodes so capacity and load scale out, and so that
adding, removing or losing a node only disturbs ~1/N of the keys instead of
nearly all of them (what `hash(key) % N` does).

## Components and responsibilities
- **ring** — sorted slice of (hash, node) virtual-node entries; lookup is a binary
  search (O(log V·N)) with wrap-around. Hash is XXH64, implemented in the repo and
  verified against the reference library. Equal hashes tie-break by node ID so
  every process builds an identical ring from the same node set.
- **store** — map + doubly linked list (LRU) behind one mutex; TTL checked lazily
  on access, plus a janitor that samples keys so unread expired keys are freed.
- **resp / server** — RESP2 framing, one goroutine per connection, replies flushed
  only when no pipelined input is buffered. Read/write deadlines on every command.
- **coordinator** — receives heartbeats, marks nodes dead after a timeout,
  maintains a version number that increments only on membership changes, serves
  the ring by long-poll and keeps a bounded event log.
- **client** — local ring copy, per-node connection pools, retries once on a stale
  pooled connection, never retries timeouts, follows the coordinator.

## Decisions and their downsides
| Decision | Why | Downside / answer to "what did that cost?" |
|---|---|---|
| Client-side ring | No extra hop per request; coordinator is off the data path | A client's ring can be briefly stale: a key may go to the wrong node, which is a cache miss, not wrong data |
| Coordinator + heartbeats (not gossip) | Simple, easy to reason about and demo | Single point of failure. Clients keep their last ring if it dies, but membership freezes. Production: Raft/etcd, or SWIM gossip |
| `(epoch, version)` pair | Version restarts at 0 when the coordinator restarts; the epoch tells a new run from "no change" | Slightly more protocol |
| Idempotent heartbeat as join/keep-alive/rejoin | A node never needs to know if the coordinator restarted or marked it dead | Address changes bump the version too |
| Node ID separate from address | A node restarting on a new port keeps its ring positions | Operator must keep IDs unique and stable |
| Availability over consistency | It is a cache: a miss falls back to the database | Reads can be stale in races; TTL + invalidate-on-write bound it |
| No replication | Keeps MVP small; crash = those keys are cold | Lost keys cause a burst of DB load (thundering herd). Stretch: replicate to next R nodes |
| Raw TCP + RESP subset | `redis-cli` and `redis-benchmark` work unchanged | Only ~12 commands. Say "RESP-compatible subset", not "Redis-compatible" |
| Full-table mutex in store | Obviously correct | Contention on many cores. Stretch: sharded locks |
| XXH64 written in-repo | Zero dependencies; tested against reference vectors | Not a vetted library; swap to `cespare/xxhash` if preferred (same function) |

## Failure semantics (what actually happens)
- **Node crashes:** its connections break. Client calls to it return `*NodeError`
  (caller falls back to the DB). After the heartbeat timeout the coordinator marks
  it dead, bumps the version, the client's long-poll returns, and the ring is
  rebuilt without it. Only that node's keys are lost; no surviving key moves.
- **Node restarts:** same ID gives the same ring positions, so its old key range
  routes to it again, to an empty cache (misses until refilled).
- **Graceful leave:** node calls `/deregister`; the ring changes immediately.
- **Coordinator down:** clients use their last ring, retry with exponential
  backoff; nodes keep serving and keep retrying heartbeats.
- **Slow node:** timeouts bound the damage; a timed-out request is not retried.

## Measured results (docs/results)
- Adding a node moves ~1/(N+1) of keys with consistent hashing and ~N/(N+1) with
  modulo hashing (e.g. 10→11 nodes: ~9.9% vs ~90.9%).
- Load imbalance (std dev / mean, 10 nodes): ~89% at 1 vnode, ~8% at 150, ~4% at 500.
- Single node, one machine, `redis-benchmark`: ~65–85k ops/s unpipelined, hundreds
  of thousands with `-P 16`. Relative numbers only — client and server share the host.

## Known limitations (say them before the interviewer does)
Single coordinator; no replication or persistence; no authentication or TLS; one
mutex per store; item-count (not byte) bound on memory; the 150-vnode default is
a measured trade-off, not a universal constant.

## Likely interview questions
1. **Why consistent hashing?** `hash % N` remaps ~all keys when N changes; the ring moves ~1/N.
2. **Why virtual nodes?** One point per node is lumpy (89% imbalance measured); 150 gives ~8%.
3. **How do you tell a dead node from a slow one?** You can't, exactly: heartbeat timeout trades detection time against false positives. Choose timeout ≥ 3× the interval.
4. **What if a client's ring is stale?** Wrong node → miss, not wrong data. It converges on the next long-poll return.
5. **Coordinator single point of failure?** Yes; data path survives, membership freezes. Fix: Raft/etcd or gossip.
6. **Why no retry on timeout?** It doubles tail latency and the node is probably overloaded. A reused-connection failure that returns instantly is retried once on a fresh connection.
7. **What happens to hot keys / stampedes?** Not handled in the cache; the demo app will use `singleflight`.
8. **CAP?** Availability over consistency; a cache can serve a miss.
