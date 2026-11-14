# Architecture

## Modules

```mermaid
flowchart LR
  subgraph core["core (no framework, no threads, no clock)"]
    RN[RaftNode] --> RS[(RaftStorage)]
    RN --> SM[KvStateMachine]
    RN -. Transport / Scheduler .-> X(( ))
  end
  subgraph simulation["simulation = FaultLab engine"]
    SC[SimCluster<br/>virtual clock, seeded network] --> RN
    SR[ScenarioRunner + SimClient] --> SC
    CK[SafetyChecker<br/>LinearizabilityChecker] --- SR
    AN[RunAnalyzer · Explainer · RunComparison] --- SR
  end
  subgraph backend["backend = one node process"]
    NR[NodeRuntime<br/>single node thread] --> RN
    GT[GrpcTransport] <--> NR
    API[REST controllers] --> NR
    LF[LiveFaultService] --> GT
    LE[LiveExperimentService] --> CK
    FL[FaultLab API] --> SR
  end
  frontend["frontend (React)"] -->|HTTP /api| API
  benchmarks["benchmarks (load generator)"] -->|HTTP| API
```

| Module | Responsibility | Depends on |
|---|---|---|
| `core` | Raft (`RaftNode`), storage (`FileStorage`, `MemoryStorage`, `FaultyStorage`), KV state machine | nothing |
| `simulation` | Deterministic simulator, scenarios, safety and linearizability checkers, analysis | `core` |
| `backend` | Spring Boot node: gRPC transport, REST API, metrics, live faults and experiments | `core`, `simulation` |
| `benchmarks` | Closed-loop HTTP load generator | nothing (public API only) |
| `frontend` | Dashboard | REST API |

The key design decision is that **`RaftNode` has no hidden dependencies**: it talks to the world only
through four small interfaces (`Scheduler`, `Transport`, `RaftStorage`, `StateMachine`). The server plugs in
a real thread, gRPC and files; the simulator plugs in a virtual clock, a seeded network and in-memory disks.
There is one consensus implementation, and FaultLab tests exactly the code that runs in production mode.

## One node at runtime

```mermaid
flowchart TB
  HTTP[HTTP threads<br/>Tomcat] -- enqueue + await future --> Q[[node thread queue]]
  GRPC[gRPC threads] -- validated messages --> Q
  Q --> NT[node thread]
  NT --> RAFT[RaftNode]
  RAFT --> WAL[(WAL segments<br/>hardstate<br/>snapshots)]
  RAFT --> KV[KvStateMachine]
  RAFT -- one-way envelopes --> OUT[GrpcTransport<br/>async, per-peer in-flight cap]
  NT --> EV[EventLog ring buffers]
```

- **One thread owns all Raft state** (`NodeRuntime`). HTTP and gRPC threads never touch it; they submit
  tasks and wait on futures. There are no locks inside the consensus code.
- **Group commit**: concurrent writes are queued and proposed as one batch per node-thread turn: one WAL
  append and one fsync for the whole batch.
- **Durability ordering**: `RaftStorage` methods return only after the data is durable, and Raft persists
  before it sends anything that depends on the write (votes, acknowledgements).
- **Fail-stop**: any storage error stops the Raft node rather than continuing on state it cannot trust.

## Write path

```mermaid
sequenceDiagram
  participant C as Client
  participant F as Follower (any node)
  participant L as Leader
  participant P as Other followers
  C->>F: PUT /api/v1/kv/k
  F->>F: propose -> NOT_LEADER (hint L)
  F->>L: forward HTTP request
  L->>L: append to WAL (fsync, batched)
  L->>P: AppendEntries (gRPC)
  P->>P: append + fsync
  P-->>L: AppendEntriesReply(match)
  L->>L: majority match -> commit -> apply
  L-->>F: 201 {index, term, version}
  F-->>C: 201
```

A write is **committed** when a majority stores it (and it is from the leader's term), **applied** when the
leader executes it on its state machine, and **acknowledged** only after both. See
[CONSISTENCY_MODEL.md](CONSISTENCY_MODEL.md).

## Deployment

```mermaid
flowchart LR
  B[Browser] --> N[nginx: dashboard + /api proxy]
  N --> A1[node1:8080]
  N -.backup.-> A2[node2:8080]
  N -.backup.-> A3[node3:8080]
  A1 <-->|gRPC 9100| A2
  A2 <-->|gRPC 9100| A3
  A1 <-->|gRPC 9100| A3
  A1 --- V1[(volume n1)]
  A2 --- V2[(volume n2)]
  A3 --- V3[(volume n3)]
```

`docker-compose.yml` runs three nodes with persistent volumes, the dashboard behind nginx, and an
optional Prometheus (`--profile metrics`). `scripts/cluster.sh` does the same with plain processes.

## Concurrency assumptions (summary)

| Component | Threads | Shared state protection |
|---|---|---|
| `RaftNode`, `KvStateMachine`, `FileStorage` | node thread only | none needed |
| `EventLog`, `OperationLog`, `RequestMetrics` | any | `synchronized` |
| `NetworkFaults` | any | concurrent collections, volatile fields |
| `GrpcTransport` stats | gRPC + node thread | atomics |
| `LiveFaultService` | HTTP threads + expiry thread | `synchronized` apply/revert |
| Simulator | exactly one thread | none (determinism depends on it) |
