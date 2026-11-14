# Benchmark results

Measured 20261113-131925 UTC by `scripts/bench.py`; raw JSON in `benchmarks/results/20261113-131925/`.

- Machine: Apple M5, 10 cores, 24.0 GB RAM, Darwin 27.0.0
- JVM: openjdk version "21.0.12.1" 2026-08-18 LTS; -Xmx512m (per node), defaults otherwise
- Setup: load generator and all nodes share one machine; networking is localhost. Closed-loop clients (no think time), 3 s warm-up, 15 s measured per configuration, fresh cluster per node count.
- Durability: fsync on every WAL append unless the label says `no-fsync`.
- Latency is client-side round trip in milliseconds; throughput counts successful requests only.

| Configuration | Nodes | Clients | Writes/s | Write p50 | p95 | p99 | Reads/s | Read p50 | p95 | p99 | Errors | Node CPU mean / max % | Max heap MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|
| write-n1-c1 | 1 | 1 | 242.133 | 4.001 | 5.055 | 5.94 | 0.0 | 0 | 0 | 0 | 0 | 3.0 / 4.8 | 51.8 |
| write-n1-c16 | 1 | 16 | 2022.4 | 7.969 | 8.322 | 10.019 | 0.0 | 0 | 0 | 0 | 0 | 6.2 / 19.1 | 68.7 |
| write-n1-c64 | 1 | 64 | 7874.467 | 7.973 | 9.554 | 14.675 | 0.0 | 0 | 0 | 0 | 0 | 12.9 / 16.2 | 125.2 |
| write-n3-c1 | 3 | 1 | 97.4 | 10.518 | 12.026 | 12.917 | 0.0 | 0 | 0 | 0 | 0 | 2.7 / 6.6 | 52.3 |
| write-n3-c16 | 3 | 16 | 788.333 | 19.703 | 28.261 | 32.763 | 0.0 | 0 | 0 | 0 | 0 | 17.8 / 43.5 | 160.2 |
| write-n3-c64 | 3 | 64 | 2870.0 | 21.859 | 30.153 | 36.705 | 0.0 | 0 | 0 | 0 | 0 | 12.5 / 36.3 | 158.9 |
| read-linearizable-n3-c32 | 3 | 32 | 0.0 | 0 | 0 | 0 | 31174.533 | 0.956 | 1.344 | 2.338 | 0 | 9.2 / 41.6 | 170.8 |
| read-leader-n3-c32 | 3 | 32 | 0.0 | 0 | 0 | 0 | 59057.467 | 0.502 | 0.829 | 1.41 | 0 | 7.0 / 43.3 | 170.4 |
| read-stale-n3-c32 | 3 | 32 | 0.0 | 0 | 0 | 0 | 54846.133 | 0.534 | 0.932 | 1.563 | 0 | 8.5 / 29.7 | 182.9 |
| mixed-50-50-n3-c32 | 3 | 32 | 450.267 | 29.213 | 44.101 | 51.081 | 443.667 | 39.392 | 66.402 | 74.599 | 0 | 13.3 / 49.9 | 304.9 |
| write-1KiB-n3-c32 | 3 | 32 | 1456.733 | 21.166 | 30.826 | 37.737 | 0.0 | 0 | 0 | 0 | 0 | 20.2 / 41.4 | 261.8 |
| write-via-followers-n3-c32 | 3 | 32 | 1565.333 | 19.546 | 30.829 | 37.707 | 0.0 | 0 | 0 | 0 | 0 | 4.7 / 9.3 | 181.7 |
| write-n5-c1 | 5 | 1 | 96.333 | 10.151 | 15.72 | 18.244 | 0.0 | 0 | 0 | 0 | 0 | 2.5 / 7.3 | 55.8 |
| write-n5-c16 | 5 | 16 | 592.333 | 26.116 | 37.446 | 48.767 | 0.0 | 0 | 0 | 0 | 0 | 11.6 / 36.9 | 157.6 |
| write-n5-c64 | 5 | 64 | 2126.333 | 29.739 | 39.495 | 46.54 | 0.0 | 0 | 0 | 0 | 0 | 9.5 / 35.8 | 183.3 |
| write-n3-c64-no-fsync | 3 | 64 | 26517.067 | 1.975 | 4.573 | 8.102 | 0.0 | 0 | 0 | 0 | 0 | 20.3 / 50.7 | 181.6 |
| write-n3-c32-leader-killed | 3 | 32 | 1498.867 | 20.7 | 27.569 | 32.785 | 0.0 | 0 | 0 | 0 | {"IOException": 32, "ConnectException": 143} | 18.5 / 37.0 | 104.6 |
| write-n3-c32-leader-isolated | 3 | 32 | 1385.467 | 22.069 | 29.834 | 33.261 | 0.0 | 0 | 0 | 0 | {"OUTCOME_UNKNOWN": 32} | 14.7 / 35.8 | 165.9 |

## Fault runs

- **write-n3-c32-leader-killed**: SIGKILL leader n1 at 1794576372.478. Longest window with zero successful requests: **300 ms** (100 ms buckets). Errors: {"IOException": 32, "ConnectException": 143}.
- **write-n3-c32-leader-isolated**: ISOLATE [n1] for 3000 ms. Longest window with zero successful requests: **300 ms** (100 ms buckets). Errors: {"OUTCOME_UNKNOWN": 32}.

## Storage after `write-n3-c64`

| Node | Disk bytes | Keys | Last index | Snapshot index |
|---|---:|---:|---:|---:|
| n1 | 14631755 | 9984 | 66860 | 60133 |
| n2 | 14631755 | 9984 | 66860 | 60091 |
| n3 | 14631755 | 9984 | 66860 | 60155 |
