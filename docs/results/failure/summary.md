Load: 400 req/s open-loop for 1m30s, Zipf s=1.10 over 1000 keys (seed 1); the first 10s is excluded from the table.

| Phase | Window | Requests/s | Hit ratio | p50 ms | p95 ms | p99 ms | Max ms | Errors |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| before: crash node3 | 10–30 s | 400 | 96.3% | 2.4 | 4.1 | 9.5 | 15.8 | 0 |
| after: crash node3 | 30–60 s | 400 | 93.2% | 2.6 | 9.1 | 508.9 | 521.9 | 0 |
| after: restart node3 | 60–90 s | 400 | 93.8% | 2.5 | 8.5 | 9.7 | 12.1 | 0 |

Event impact (hit ratio, per second):

- **crash node3** (at 30 s): hit ratio fell to 53.0% in its worst second (was 96.3%) and was back within 5 points after 3 s.
- **restart node3** (at 60 s): hit ratio fell to 85.8% in its worst second (was 96.3%) and was back within 5 points after 6 s.

Hot keys: the 10 hottest keys received 48% of requests and the hottest 10% (100 keys) received 77%.

Database reads during the run: 2218 (62 per 1000 requests); cache errors seen by the app: 617.

Share of requests by owning cache node (before the first event): node1 25% node2 28% node3 47%.
