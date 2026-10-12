# cachekit — a distributed cache with consistent hashing

A from-scratch distributed in-memory cache in Go. Keys are spread over cache
nodes with a **consistent-hash ring (virtual nodes)**, nodes speak a **Redis-
compatible protocol (RESP subset)** over raw TCP, and a **coordinator** detects
dead nodes by heartbeat and publishes a versioned ring that clients follow.
No external Go dependencies.

```
                       heartbeats / ring (HTTP+JSON)
   Nodes (RESP/TCP) ───────────────►  Coordinator  ◄──── long-poll /ring ────┐
     ▲   ▲   ▲                                                               │
     └───┴───┴──── GET/SET/DEL (RESP) ─── Client library (local ring copy) ──┘
                                              ▲
                                       your app / cachectl
```

Key lookups happen **inside the client** with a local copy of the ring, so no
request pays an extra network hop. The coordinator is control plane only.

## Layout

| Path | What it is |
|---|---|
| `ring/` | Consistent-hash ring + vnodes + XXH64. Pure logic, no I/O. |
| `store/` | LRU + TTL in-memory store (one per node). |
| `resp/` | RESP reader/writer (the wire protocol). |
| `server/` | TCP server for one node; implements the commands. |
| `cluster/` | Types shared by coordinator and client (membership, events). |
| `coordinator/` | Heartbeats, failure detection, versioned membership, HTTP API. |
| `node/` | A node's lifecycle: store + server + heartbeat loop. |
| `client/` | Pooled client with local ring; follows the coordinator. |
| `cmd/node`, `cmd/coordinator` | The two server programs. |
| `cmd/cachectl` | CLI to poke a cluster (`status`, `get`, `set`, `fill`, `check`...). |
| `cmd/experiment` | Produces the report graphs' data (remap % and load balance). |
| `deploy/` | Dockerfile + docker-compose (coordinator + 3 nodes). |
| `docs/` | `DESIGN.md` (decisions, trade-offs, interview Q&A), results + plot script. |

## Run the tests

```powershell
go vet ./...
go test ./...
```

`-race` needs a C compiler on Windows (cgo), so it is optional there:
`go test -race ./...` works on Linux/macOS/WSL.

## Run a cluster (no Docker) — 4 terminals, in the project folder

```powershell
go build -o bin/ ./cmd/...
```
```powershell
.\bin\coordinator.exe                                                       # terminal 1
.\bin\node.exe -id node-1 -listen :7001 -coordinator http://localhost:8080  # terminal 2
.\bin\node.exe -id node-2 -listen :7002 -coordinator http://localhost:8080  # terminal 3
.\bin\node.exe -id node-3 -listen :7003 -coordinator http://localhost:8080  # terminal 4
```
(Windows Firewall may ask once: allow on private networks.)

Then in a 5th terminal:

```powershell
.\bin\cachectl.exe status          # 3 nodes, ring share per node
.\bin\cachectl.exe fill 10000      # spreads 10000 keys, prints keys per node
.\bin\cachectl.exe check 10000     # 100% hits
.\bin\cachectl.exe set hello world 30s
.\bin\cachectl.exe get hello
.\bin\cachectl.exe owner hello
```

**Failover demo**

1. Close the **terminal window** of node-2 (or kill its process). This is a *crash*: no goodbye.
2. `.\bin\cachectl.exe check 10000` right away: about a third of requests report *errors* (node unreachable, the coordinator has not noticed yet).
3. Wait ~3 seconds, run `check` again: **0 errors**, hits drop to about 2/3 (node-2's keys are gone), and the keys on node-1/node-3 are untouched — that is the consistent-hashing result.
4. Start node-2 again (even on another port, same `-id node-2`): it gets its ring positions back. `status` shows it alive.
5. Press **Ctrl+C** in node-3's terminal instead: that is a *graceful* leave and is picked up immediately.

`.\bin\cachectl.exe status` always shows who is alive and how much of the ring each node owns.

## Redis tools without installing Redis

Nodes speak RESP, so the real Redis tools work. Using the official image from Docker Desktop (nothing to install):

```powershell
docker run --rm redis:7 redis-cli -h host.docker.internal -p 7001 ping
docker run --rm redis:7 redis-cli -h host.docker.internal -p 7001 set foo bar ex 100
docker run --rm redis:7 redis-benchmark -h host.docker.internal -p 7001 -t set,get -n 100000 -q
docker run --rm redis:7 redis-benchmark -h host.docker.internal -p 7001 -t set,get -n 100000 -P 16 -q
```

This talks to a **single node**; `redis-cli` does not know about the ring.

## Run with Docker Compose

```powershell
docker compose -f deploy/docker-compose.yml up --build
docker compose -f deploy/docker-compose.yml run --rm cachectl status
docker compose -f deploy/docker-compose.yml run --rm cachectl fill 10000
docker stop cachekit-node2-1                      # crash a node
docker compose -f deploy/docker-compose.yml run --rm cachectl check 10000
docker start cachekit-node2-1                     # bring it back
```
Container names can differ slightly by Docker version; `docker ps` shows them.
`cachectl` runs inside the compose network because nodes advertise `node1:7001`
etc., which only resolves there. From the host, `curl http://localhost:8080/status`
shows the cluster and nodes are also published on ports 7001–7003.

## Reproduce the headline graphs

```powershell
go run ./cmd/experiment -out docs/results
pip install matplotlib
python docs/plot_results.py docs/results
```
Writes `remap.csv/png` (keys moved when a node is added: consistent hashing vs
`hash % N`) and `balance.csv/png` (load imbalance vs number of vnodes). Results
from a reference run are already in `docs/results/`.

## Coordinator HTTP API

`POST /heartbeat`, `POST /deregister`, `GET /ring` (add `?since=V&epoch=E&wait=10s`
to long-poll), `GET /status`, `GET /events`, `GET /healthz`.

## Status vs. the roadmap

Done: ring (phase 1), store (2), RESP server (3), client (4), coordinator (5),
Docker Compose cluster, the two experiments, `redis-benchmark` compatibility.
Not built yet: the PostgreSQL cache-aside demo app (phase 6), the Zipfian load
generator and failure-injection graphs (7), the web dashboard (8).
