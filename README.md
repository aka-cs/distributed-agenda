# Distributed Agenda

Distributed Agenda is a distributed scheduling platform built for the **Distributed Systems** course.  
It combines a Go backend (gRPC + custom Chord DHT) with a Python Streamlit client to support:

- user authentication and profile management,
- group creation and membership with role hierarchy,
- collaborative event planning and confirmations,
- distributed history/audit tracking,
- node discovery and decentralized data placement.

> Wiki reference: https://github.com/aka-cs/distributed-agenda/wiki  
> The wiki is intentionally brief; this README captures the implementation-level context needed to understand and run the project effectively.

---

## 1) What this project demonstrates

This is not a simple CRUD app. It showcases practical skills in:

- **Distributed systems engineering** (ring topology, successor/predecessor maintenance, key partitioning and replication).
- **Service-oriented backend design** using **gRPC + Protocol Buffers**.
- **Concurrency control** in Go (`goroutines`, `sync.RWMutex`, periodic maintenance threads).
- **Security fundamentals** (bcrypt password hashing, RSA-signed JWT authentication, request interceptors).
- **Client resiliency patterns** (server discovery, request queue, conflict tracking, eventual synchronization).
- **Data serialization and persistence** through protobuf payloads over a distributed key-value substrate.

If you are evaluating engineering depth, the core value is the combination of **distributed storage concerns** and **application-level collaboration workflows** in one system.

---

## 2) Repository structure

```text
<project-root>
├── client/                  # Streamlit frontend + async gRPC client
├── server/                  # Go backend services + Chord implementation
├── proto/                   # Shared protobuf contracts (auth/users/groups/events/history)
├── doc/                     # Basic run instructions
├── makefile                 # Top-level commands
└── README.md
```

Key implementation areas:

- **Chord internals:** `server/chord/`
- **Application services:** `server/services/`
- **Service bootstrap:** `server/main/main.go`, `server/services/start.go`
- **Client app entrypoint:** `client/app.py`
- **Client RPC utilities:** `client/rpc/`

---

## 3) Architecture overview

### Backend (Go)

The backend starts:

1. a Chord node on `50050` (distributed storage/lookup layer), and
2. five gRPC application services on dedicated ports:
   - `50051` Users
   - `50052` Groups
   - `50053` Events
   - `50054` Auth
   - `50055` History

Source: `server/services/start.go`.

### Client (Python + Streamlit)

The Streamlit client loads a multi-page interface and starts background workers to:

- discover servers in LAN-like contexts,
- process queued requests (including conflict handling),
- synchronize history periodically.

Source: `client/app.py`, `client/rpc/client.py`, `client/rpc/requests_queue.py`.

---

## 4) Data and service contracts

Protocol Buffers in `/proto` define system boundaries:

- `auth.proto` – login/signup contract.
- `users.proto` – get/edit user profile.
- `groups.proto` – group lifecycle + role-based member operations.
- `events.proto` – create/get/delete/confirm/reject events.
- `history.proto` – append and stream audit entries.

Because both server and client are generated from these contracts, API drift is minimized and service interfaces stay explicit.

Generate bindings with:

```bash
cd <project-root>
make pbc
```

---

## 5) Setup and execution

### Prerequisites

- Python 3.10+
- Go 1.18+
- `protoc` (if regenerating protobuf files)

### Install dependencies

```bash
cd <project-root>
make server-install
make client-install
```

### Run server

```bash
cd <project-root>
make server
```

### Run client

```bash
cd <project-root>
make client
```

---

## 6) Functional usage flow

A typical end-to-end workflow:

1. **Sign up / login** through Auth service (`auth.proto`, `server/services/auth_service.go`).
2. Client stores JWT and attaches it automatically to outgoing requests (`client/rpc/client.py`).
3. **Create groups** and assign users/admin levels (`groups.proto`, `server/services/groups_service.go`).
4. **Create and coordinate events**; users can confirm or reject participation (`events.proto`, `server/services/events_service.go`).
5. **History service** records actions like create/update/delete/confirm/reject, enabling activity traceability (`history.proto`, `server/services/history_service.go`).

---

## 7) Implementation considerations (important)

### 7.1 Chord ring lifecycle and discovery

- Node identity is derived by hashing `IP:port` with SHA-1 (160-bit space).
- On start, a node attempts UDP broadcast discovery (`Chord?` / `I am chord`) over port `8830`.
- If another node responds, it joins that ring; otherwise it initializes a new ring.

Relevant files: `server/chord/node_internal.go`, `server/chord/configuration.go`, `server/chord/utils.go`.

### 7.2 Stabilization and fault-tolerance threads

The node runs periodic routines for:

- predecessor/successor health checks,
- ring stabilization,
- finger table repair,
- successor queue repair,
- key placement correction.

These are critical to maintain eventual correctness under node churn.  
Relevant file: `server/chord/node_threads.go`.

### 7.3 Replication strategy

- Storage writes are replicated to successors.
- Successor queues (configurable; default includes multiple nodes) support resilience.
- Partition/extend/discard operations allow key redistribution when topology changes.

Relevant files: `server/chord/node.go`, `server/chord/storage.go`, `server/chord/node_internal.go`.

### 7.4 Storage abstraction

Two storage modes are implemented behind one interface:

- in-memory dictionary (`Dictionary`),
- disk-backed dictionary (`DiskDictionary`, used by default).

The abstraction isolates storage policy from routing and service logic.  
Relevant file: `server/chord/storage.go`.

### 7.5 Security model

- Passwords are hashed (bcrypt) before persistence.
- Auth service issues RSA-signed JWTs.
- Unary/stream interceptors validate tokens and enrich request context.

Relevant files: `server/services/auth_service.go`, `server/services/interceptors.go`, `server/pv.pem`, `server/pub.pem`.

### 7.6 Client-side resilience and eventual consistency

The client is intentionally defensive:

- it discovers active servers and rotates targets,
- queues requests when immediate processing fails,
- tracks event conflicts and retries,
- periodically refreshes history.

This pattern makes UI interactions more robust under transient outages, while matching distributed backends where immediate consistency is not always guaranteed.

---

## 8) Skills reflected in the implementation

This project reflects competency in:

1. **Distributed algorithms:** practical Chord implementation details beyond theory (join, stabilize, finger maintenance, partitioning).
2. **Backend microservice decomposition:** clear service responsibilities with protobuf-driven contracts.
3. **Concurrent programming:** lock discipline and asynchronous background maintenance.
4. **Network programming:** gRPC RPC calls + UDP broadcast discovery.
5. **Security engineering:** JWT + RSA + interceptor-based enforcement.
6. **State synchronization design:** request queues, replay logic, and history/event convergence.
7. **Cross-language integration:** Go backend + Python client sharing protocol contracts.

---

## 9) Notes for contributors

- Keep protobuf definitions as the source of truth for API contracts.
- Any service interface change should update generated stubs and both server/client call sites.
- For distributed behavior changes (Chord, replication, maintenance intervals), validate both single-node and multi-node scenarios.
- Prefer incremental modifications in `server/chord` because many routines are interdependent.

---

## 10) Quick command reference

```bash
# From repo root:
cd <project-root>

# Install dependencies
make server-install
make client-install

# Generate protobuf bindings
make pbc

# Run
make server
make client
```
