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
| Per-node circuit breaker in the client | A dead-but-not-yet-evicted node would otherwise cost every request a full timeout | A node that hiccups once is skipped for a cooldown (1 s), so its keys go to the database briefly; thresholds (3 failures, 1 s) are tuned for this demo, not universal |
| Full-table mutex in store | Obviously correct | Contention on many cores. Stretch: sharded locks |
| XXH64 written in-repo | Zero dependencies; tested against reference vectors | Not a vetted library; swap to `cespare/xxhash` if preferred (same function) |

## Demo app: cache-aside (catalog/)
- **Read:** cache hit returns; miss loads from PostgreSQL (one load per key at a time,
  see `flight/`), stores the result with a TTL (60 s ±10% jitter), returns it.
- **Write:** update PostgreSQL first, then **delete** the cached key (not "update the
  cache"): the next read reloads it. A failed database write never touches the cache.
- **Cache is an optimisation, not a dependency:** any cache error (node down, timeout)
  is counted, logged at most every 5 s, and the request is served from the database.
  A failed invalidation leaves the old copy until its TTL expires; the TTL is what bounds staleness.
- **Negative caching:** a missing id is cached as a marker for 15 s so repeated lookups
  of ids that do not exist do not each reach the database. `Create` invalidates the new
  id's key so a stale "not found" cannot hide a new product.
- **TTL jitter:** keys cached together would expire together and stampede the database.
- **Single-flight:** N concurrent misses on one key make one database call. The shared
  load uses its own timeout and does not inherit the first caller's context, so one
  cancelled request cannot fail the others.
- **Known race (accepted):** reader misses and loads the old row; a writer updates the
  row and deletes the key; the reader then caches the old row. Stale for at most one TTL.
  Fixes if it mattered: versioned values / compare-and-set, or shorter TTLs.
- **Cached values are validated:** an entry that does not parse, or whose id differs from
  its key, is dropped and treated as a miss.
- Not covered: list pages are not cached; a hot key still concentrates on one node.

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

## Load testing method (cmd/loadgen)
- **Zipf keys (s = 1.1, 1000 keys):** realistic skew; the hottest ~10 keys take about half
  the reads. Seeded, so a run is reproducible and node placement of the hot keys is fixed.
- **Open loop, latency from the scheduled send time:** avoids coordinated omission. A closed
  loop under-reports the very latency a failure causes.
- **Latency percentiles are over successful requests only;** errors are counted separately so
  a timeout cannot flatter a percentile.
- **Simulated DB latency (`DB_LATENCY`):** local Postgres is ~1 ms, which makes the cache look
  pointless. The simulated delay is applied to every database call and every result is labelled.
- **Hit ratio counts a database fallback as a miss,** so a dead cache node shows up as a hit-ratio
  dip, not as errors. "Recovery time" = seconds from the event until the per-second hit ratio is
  within 5 points of the pre-event baseline for 3 consecutive seconds.
- **What bounds the dip:** detection (the coordinator's heartbeat timeout, 3 s here) plus refill
  (each moved key costs one miss). A graceful stop deregisters first and skips the detection window.
- **Limits:** one machine (generator, app, Docker share CPU), one run, 1-second buckets.

## Measured results: load test (cmd/loadgen; Windows laptop, Docker Desktop, 5 ms simulated DB latency)
- 500 req/s, Zipf hot keys: cache 98.0% hit ratio, p50 2.4 ms vs 8.1 ms without cache; database reads 48 vs 908 per 1000 requests (about 95% fewer).
- Hard-killing the node that owns about 47% of requests (400 req/s): 0 failed requests, hit ratio down to 53% in the worst second and back within 5 points after 3 s; restart dip to 86%, back after 6 s.
- During the crash window p99 reached about 509 ms: two 250 ms cache timeouts (failed GET + failed SET) per request routed to the dead node until the coordinator removes it. Fixed by (1) not refilling the cache after a failed read and (2) a per-node circuit breaker in the client (see the README).

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
7. **What happens to hot keys / stampedes?** Not handled in the cache itself; the demo app coalesces concurrent misses per key (`flight/`) and jitters TTLs. A single very hot key still lives on one node.
8. **CAP?** Availability over consistency; a cache can serve a miss.
