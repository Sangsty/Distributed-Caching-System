Load: 500 req/s open-loop for 40s, Zipf s=1.10 over 1000 keys (seed 1); the first 10s is excluded from the table.

| Phase | Window | Requests/s | Hit ratio | p50 ms | p95 ms | p99 ms | Max ms | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| steady state | 10–40 s | 500 | 98.0% | 2.4 | 3.3 | 9.0 | 12.8 | 0 |

Hot keys: the 10 hottest keys received 48% of requests and the hottest 10% (100 keys) received 77%.

Database reads during the run: 958 (48 per 1000 requests); cache errors seen by the app: 0.

Share of requests by owning cache node (after the warm-up): node1 25% node2 29% node3 46%.
