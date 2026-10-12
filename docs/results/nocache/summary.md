Load: 500 req/s open-loop for 40s, Zipf s=1.10 over 1000 keys (seed 1); the first 10s is excluded from the table.

| Phase | Window | Requests/s | Hit ratio | p50 ms | p95 ms | p99 ms | Max ms | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| steady state | 10–40 s | 500 | 0.0% | 8.1 | 9.0 | 10.2 | 28.2 | 0 |

Hot keys: the 10 hottest keys received 48% of requests and the hottest 10% (100 keys) received 77%.

Database reads during the run: 18155 (908 per 1000 requests); cache errors seen by the app: 0.
