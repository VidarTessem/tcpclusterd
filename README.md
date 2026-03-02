# Distributed Cluster Array Server

A production-ready distributed cluster system with multiple access protocols (HTTP/REST, WebSocket, TCP) and built-in peer synchronization. Perfect for building distributed caches, state management systems, or inter-service communication layers.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Features](#features)
3. [Configuration](#configuration)
4. [HTTP API](#http-api)
5. [WebSocket Protocol](#websocket-protocol)
6. [TCP Protocol](#tcp-protocol)
7. [IP Whitelisting](#ip-whitelisting)
8. [Clustering & Peer Sync](#clustering--peer-sync)
9. [Security](#security)
10. [Commands](#commands)
11. [Examples](#examples)
12. [Architecture](#architecture)

---

## Quick Start

### 1. Initialize Configuration

First time running the app? Create a default `.env` file with randomly generated security keys:

```bash
./app init
```

This creates `.env` with:
- All required configuration variables
- Randomly generated AES-256 cipher key for encryption
- Random peer secret for cluster authentication
- Random TCP authentication key
- Default values allowing all IPs (safe for development)

### 2. Start the Server

```bash
./app
```

You'll see:
```
[CLUSTER] Runtime directory: runtime/2026-03-01_21-02-16
[CLUSTER] Peer sync server started on [::]:9000
[TCP] Server started on [::]:9001
Starting HTTP server on [::]:8888
[CLUSTER] Peer authenticated from [::1]:60465
```

### 3. Test HTTP API

```bash
# Write data
curl -X POST "http://localhost:8888/cluster/write?array=users&key=user1&value=john_doe"

# Read data
curl "http://localhost:8888/cluster/read?array=users&key=user1"

# Get all data
curl "http://localhost:8888/cluster/all"

# Real-time updates via WebSocket
wscat -c "ws://localhost:8888/ws?array=users"
```

---

## Features

### Multi-Protocol Access
- **HTTP/REST** - Standard HTTP endpoints for CRUD operations
- **WebSocket** - Real-time streaming of array changes
- **TCP** - Direct line-based protocol for efficient communication
- **Optional** - Enable/disable each protocol independently

### Clustering
- **Peer Synchronization** - Automatic data sync with other cluster nodes
- **Encryption** - All data encrypted in transit (AES-256-GCM)
- **Authentication** - Shared secrets and IP whitelisting
- **Automatic Failover** - Syncs with multiple peers on startup

### Security
- AES-256-GCM encryption for cluster communication
- IP whitelisting with CIDR subnet support
- Per-protocol authentication keys
- Persistent file encryption
- Secure key generation on init

### Data Management
- Multiple named arrays (hash maps)
- Key-value storage per array
- Bulk import/export with JSON format
- Persistent storage with automatic recovery
- Optional WebSocket real-time updates

### Performance
- Batched persistence (150ms coalesce window)
- Connection timeouts to prevent hangs
- Efficient binary protocol for TCP
- Minimal logging in production paths

---

## Configuration

### Environment Variables (.env)

All configuration is in `.env`. Generate defaults with `./app init`, then customize:

#### HTTP Server Configuration

```bash
# Enable/disable HTTP server (default: true)
HTTP_ENABLED=true

# HTTP server port
PORT=8888

# Listen address (:: = IPv6 any, 0.0.0.0 = IPv4 any)
HTTP_LISTEN_ADDR=[::]

# IP whitelisting - who can access HTTP endpoints
# Format: comma-separated IPs or CIDR subnets
# 0.0.0.0/0 = allow any IPv4
# ::/0 = allow any IPv6
HTTP_WHITELIST_IPS=0.0.0.0/0,::/0
```

#### WebSocket Configuration

```bash
# Enable real-time updates via WebSocket
WS_ENABLED=true
```

#### Cluster Peer Configuration

```bash
# Peer nodes to sync with (comma-separated addresses)
# Format: host:port (without CLUSTER_PORT, just hostname:port)
CLUSTER_PEERS=[::1]

# Cluster sync server port
CLUSTER_PORT=9000

# Cluster sync server listen address
CLUSTER_LISTEN_ADDR=[::]

# AES-256 encryption key (32 bytes base64)
# Generate: openssl rand -base64 32
CLUSTER_CIPHER_KEY=q6BZQxonmHjGTLQsHlQCOTZB27r5VPHHhdI6+r5foLo=

# Shared secret for peer authentication
# All peers must use same value
CLUSTER_PEER_SECRET=YKAXTAWbmSQjc+yIW+RBag==

# IP whitelisting for cluster sync port
CLUSTER_WHITELIST_IPS=0.0.0.0/0,::/0
```

#### TCP Protocol Configuration

```bash
# Enable TCP direct protocol
TCP_ENABLED=true

# TCP server port
TCP_PORT=9001

# TCP server listen address
TCP_LISTEN_ADDR=[::]

# Authentication key for TCP clients
TCP_KEY=1MHgSdYRzoaMTOW1N3HlXg==

# IP whitelisting for TCP connections
TCP_WHITELIST_IPS=0.0.0.0/0,::/0
```

---

## HTTP API

All HTTP endpoints follow REST conventions. Response format is JSON.

### 1. Get All Arrays

**Request:**
```bash
GET /cluster/all
```

**Response:**
```json
{
  "arrays": {
    "users": {
      "user1": "john_doe",
      "user2": "jane_smith"
    },
    "sessions": {
      "sess_abc123": "{\"user_id\": 1, \"timeout\": 3600}"
    }
  },
  "metrics": {
    "total_arrays": 2,
    "total_keys": 3,
    "last_sync": "2026-03-01T21:02:17Z",
    "sync_count": 5
  }
}
```

### 2. Get Specific Array

**Request:**
```bash
GET /cluster/all?array=users
```

**Response:**
```json
{
  "users": {
    "user1": "john_doe",
    "user2": "jane_smith"
  }
}
```

### 3. Write (Create/Update)

**Request:**
```bash
POST /cluster/write?array=users&key=user1&value=john_doe
```

**Response:**
```json
{
  "status": "success"
}
```

**Multiple writes:**
```bash
# Create multiple users
curl -X POST "http://localhost:8888/cluster/write?array=users&key=alice&value=alice_wonder"
curl -X POST "http://localhost:8888/cluster/write?array=users&key=bob&value=bob_builder"
```

### 4. Read Single Key

**Request:**
```bash
GET /cluster/read?array=users&key=user1
```

**Response:**
```json
{
  "value": "john_doe"
}
```

**Error if key doesn't exist:**
```json
404 Not Found
```

### 5. Delete Key

**Request (delete specific key):**
```bash
DELETE /cluster/delete?array=users&key=user1
```

**Request (delete entire array):**
```bash
DELETE /cluster/delete?array=users
```

**Response:**
```json
{
  "status": "success"
}
```

---

## WebSocket Protocol

Real-time streaming of array updates. Clients connect and receive JSON updates at specified intervals.

### Connection

**URL:**
```
ws://localhost:8888/ws?array=users&interval=1000
```

**Query Parameters:**
- `array` - (optional) Watch specific array only (omit to watch all)
- `interval` - (optional) Update frequency in milliseconds (default: 1000)

### Examples

#### Watch All Arrays

```bash
wscat -c "ws://localhost:8888/ws"
```

Client receives updates every 1 second (default):
```json
{
  "arrays": {
    "users": {
      "user1": "john_doe",
      "user2": "jane_smith"
    }
  },
  "metrics": {...}
}
```

#### Watch Specific Array

```bash
wscat -c "ws://localhost:8888/ws?array=users"
```

Client receives:
```json
{
  "users": {
    "user1": "john_doe",
    "user2": "jane_smith"
  }
}
```

#### Custom Update Interval

```bash
# Update every 5 seconds
wscat -c "ws://localhost:8888/ws?array=users&interval=5000"

# Update every 500ms
wscat -c "ws://localhost:8888/ws?interval=500"
```

### Using WebSocket in JavaScript

```javascript
const ws = new WebSocket('ws://localhost:8888/ws?array=users&interval=1000');

ws.onopen = () => {
  console.log('Connected to cluster');
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Updated users:', data.users);
};

ws.onerror = (error) => {
  console.error('WebSocket error:', error);
};

ws.onclose = () => {
  console.log('Disconnected from cluster');
};
```

### WebSocket in Python

```python
import asyncio
import websockets
import json

async def watch_cluster():
    async with websockets.connect('ws://localhost:8888/ws?array=users') as ws:
        while True:
            data = await ws.recv()
            users = json.loads(data)
            print(f"Current users: {users}")

asyncio.run(watch_cluster())
```

---

## TCP Protocol

Direct line-based TCP protocol for efficient cluster-to-cluster communication or custom clients.

### Connection

```bash
# Connect to TCP server
nc localhost 9001
```

### Protocol Format

Request format: `COMMAND arg1 arg2 arg3`
Response format: `OK <json_response>` or `ERROR <message>`

### Commands

#### 1. AUTH - Authenticate

**Required first command.** Client must authenticate before any other operations.

**Request:**
```
AUTH 1MHgSdYRzoaMTOW1N3HlXg==
```

**Response (success):**
```
OK
```

**Response (failure):**
```
ERROR: Invalid key
```

#### 2. GET - Retrieve Data

**Get specific array:**
```
GET users
```

**Response:**
```
OK {"user1":"john_doe","user2":"jane_smith"}
```

**Get all arrays:**
```
GET
```

**Response:**
```
OK {"arrays":{"users":{...},"sessions":{...}},"metrics":{...}}
```

#### 3. SET - Write Key-Value

**Format:**
```
SET arrayName key value
```

**Example:**
```
SET users user1 john_doe
```

**Response:**
```
OK
```

#### 4. DEL - Delete Key

**Delete specific key:**
```
DEL users user1
```

**Response:**
```
OK
```

#### 5. DELARRAY - Delete Entire Array

**Delete all data in array:**
```
DELARRAY users
```

**Response:**
```
OK
```

#### 6. QUIT - Close Connection

**Request:**
```
QUIT
```

Connection closes. No response.

### TCP Usage Examples

#### Using netcat

```bash
# Start connection
nc localhost 9001

# Authenticate
AUTH 1MHgSdYRzoaMTOW1N3HlXg==
# Response: OK

# Write data
SET users alice alice_wonder
# Response: OK

# Read data
GET users
# Response: OK {"alice":"alice_wonder"}

# Delete
DEL users alice
# Response: OK

# Close
QUIT
```

#### Using bash with timeout

```bash
(
  echo "AUTH 1MHgSdYRzoaMTOW1N3HlXg=="
  echo "SET config version 1.0.0"
  echo "GET config"
  echo "QUIT"
) | nc localhost 9001
```

#### Using Python

```python
import socket
import json

def tcp_client():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 9001))
    
    # Authenticate
    sock.sendall(b'AUTH 1MHgSdYRzoaMTOW1N3HlXg==\n')
    response = sock.recv(1024).decode()
    print(f"Auth: {response.strip()}")
    
    # Set value
    sock.sendall(b'SET users user1 john_doe\n')
    response = sock.recv(1024).decode()
    print(f"Set: {response.strip()}")
    
    # Get value
    sock.sendall(b'GET users\n')
    response = sock.recv(4096).decode()
    print(f"Get: {response.strip()}")
    
    # Quit
    sock.sendall(b'QUIT\n')
    sock.close()

tcp_client()
```

#### Using Go

```go
package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:9001")
	defer conn.Close()
	
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	
	// Authenticate
	writer.WriteString("AUTH 1MHgSdYRzoaMTOW1N3HlXg==\n")
	writer.Flush()
	line, _ := reader.ReadString('\n')
	fmt.Println("Auth:", line)
	
	// Set value
	writer.WriteString("SET users user1 john_doe\n")
	writer.Flush()
	line, _ = reader.ReadString('\n')
	fmt.Println("Set:", line)
	
	// Get value
	writer.WriteString("GET users\n")
	writer.Flush()
	line, _ = reader.ReadString('\n')
	fmt.Println("Get:", line)
}
```

---

## IP Whitelisting

All three protocols (HTTP, Cluster, TCP) support IP whitelisting with CIDR subnet notation.

### Configuration Format

Comma-separated list of:
- Exact IP: `127.0.0.1`, `::1`
- CIDR subnets: `192.168.0.0/16`, `2a03:94e0:205b::/48`
- Any IPv4: `0.0.0.0/0`
- Any IPv6: `::/0`

### Examples

#### Allow All (Development)

```bash
HTTP_WHITELIST_IPS=0.0.0.0/0,::/0
CLUSTER_WHITELIST_IPS=0.0.0.0/0,::/0
TCP_WHITELIST_IPS=0.0.0.0/0,::/0
```

#### Restrictive (Production)

```bash
# Only localhost and internal network
HTTP_WHITELIST_IPS=127.0.0.1,::1,192.168.0.0/16,fd00::/8
CLUSTER_WHITELIST_IPS=192.168.0.0/16,10.0.0.0/8
TCP_WHITELIST_IPS=127.0.0.1,::1,172.16.0.0/12
```

#### Specific Subnets

```bash
# ISP subnet and office network
HTTP_WHITELIST_IPS=2a03:94e0:205b::/48,203.0.113.0/24,198.51.100.0/24

# Single peer in cluster
CLUSTER_WHITELIST_IPS=192.168.1.100,192.168.1.101
```

### How It Works

1. Client connects with source IP (e.g., `192.168.1.50`)
2. Server extracts IP from connection address
3. Server checks against whitelist:
   - Exact match? Allow
   - In CIDR subnet? Allow
   - Not found? Deny with 403 Forbidden (HTTP) or error message (TCP)

### CIDR Notation Reference

- `/8` - 16,777,216 addresses (entire Class A)
- `/16` - 65,536 addresses (entire Class B)
- `/24` - 256 addresses (entire Class C)
- `/32` - 1 address (single host for IPv4)
- `/48` - IPv6 subnet (common ISP allocation)
- `/128` - 1 address (single host for IPv6)

---

## Clustering & Peer Sync

Multiple cluster instances automatically synchronize data with each other.

### Setup

#### Node 1 (Master)

```bash
# .env
CLUSTER_PEERS=[::1]:9000
CLUSTER_PORT=9000
CLUSTER_LISTEN_ADDR=[::]
CLUSTER_CIPHER_KEY=<same-key-for-all-nodes>
CLUSTER_PEER_SECRET=<same-secret-for-all-nodes>
```

#### Node 2 (Slave - same network)

```bash
# .env
CLUSTER_PEERS=[::1]:9000,192.168.1.100:9000
CLUSTER_PORT=9000
CLUSTER_LISTEN_ADDR=[::]
CLUSTER_CIPHER_KEY=<same-key-as-node-1>
CLUSTER_PEER_SECRET=<same-secret-as-node-1>
```

### How Synchronization Works

1. **On Startup** - Each node connects to all configured peers and syncs all data
2. **Periodic Sync** - Every 30 seconds, nodes synchronize with peers
3. **Data Merge** - Conflicts resolved by keeping latest timestamp
4. **Automatic Encryption** - All data encrypted with AES-256-GCM during transit
5. **Persistent Storage** - Each node stores its own copy of data

### Example: Three-Node Cluster

```bash
# node1/.env
CLUSTER_PEERS=[::1]:9000,192.168.1.101:9000,192.168.1.102:9000
CLUSTER_PORT=9000
CLUSTER_CIPHER_KEY=...
CLUSTER_PEER_SECRET=cluster-secret-123

# node2/.env
CLUSTER_PEERS=[::1]:9000,192.168.1.101:9000,192.168.1.102:9000
CLUSTER_PORT=9000
CLUSTER_CIPHER_KEY=...
CLUSTER_PEER_SECRET=cluster-secret-123

# node3/.env
CLUSTER_PEERS=[::1]:9000,192.168.1.101:9000,192.168.1.102:9000
CLUSTER_PORT=9000
CLUSTER_CIPHER_KEY=...
CLUSTER_PEER_SECRET=cluster-secret-123
```

**Start all three nodes:**
```bash
node1$ ./app &
[CLUSTER] Peer authenticated from [::1]:60465
[CLUSTER] Successfully synced from peer [::1] on startup

node2$ ./app &
[CLUSTER] Peer authenticated from [::1]:60470
[CLUSTER] Successfully synced from peer [::1] on startup

node3$ ./app &
[CLUSTER] Peer authenticated from [::1]:60475
[CLUSTER] Successfully synced from peer [::1] on startup
```

**Write on Node 1:**
```bash
curl -X POST "http://localhost:8888/cluster/write?array=users&key=user1&value=john_doe"
```

**Read from Node 2 (automatic sync):**
```bash
curl "http://node2:8888/cluster/all"
# Returns the same data - synced automatically!
```

---

## Security

### Encryption

**Data in Transit (Cluster Communication):**
- Algorithm: AES-256-GCM
- Key: 32-byte random key in `.env` (CLUSTER_CIPHER_KEY)
- All peer communication encrypted

**Data at Rest:**
- Stored in `runtime/` directory
- Not encrypted on disk (add file-level encryption at OS level if needed)

### Authentication

**Cluster Peers:**
- Shared secret in CLUSTER_PEER_SECRET
- Each node must know the secret
- Prevents unauthorized cluster access

**TCP Clients:**
- TCP_KEY authentication on first command
- Prevents unauthorized TCP access

**HTTP/WebSocket:**
- IP whitelisting only
- No per-request authentication

### Key Rotation

Changing keys requires:

1. **Update all nodes** with new keys in `.env`
2. **Restart all nodes** simultaneously (downtime required)
3. **Old data still readable** if keys stored with data (currently not supported)

**For zero-downtime key rotation:** Deploy new nodes with new keys, sync data, remove old nodes.

### Secure Key Generation

```bash
# Generate a new AES-256 key
openssl rand -base64 32

# Generate a new peer secret
openssl rand -base64 16

# Generate a new TCP key
openssl rand -base64 16
```

---

## Commands

### Initialize Configuration

```bash
./app init
# Creates .env with random keys (safe defaults)
# Safe to run multiple times (won't overwrite existing .env)
```

### Start Server

```bash
./app
# Starts all enabled protocols
# Creates runtime directory with timestamp
# Connects to peers for sync
```

### Load Last Configuration

```bash
./app --lastconfig
# Starts server and loads data from most recent runtime directory
# Useful for recovery/reboot
```

### Export All Data

```bash
./app export > backup.json
# Exports all arrays as JSON to stdout
# Does not start server
# Can be redirected to file
```

### Import Data

```bash
./app import < backup.json
# Imports JSON data into new runtime directory
# Replaces existing data

./app import --clear < backup.json
# Clears existing data first, then imports
# Safer than plain import
```

---

## Examples

### Example 1: User Session Cache

Store user session data that's accessible from multiple services.

**Write session (Service A):**
```bash
curl -X POST "http://localhost:8888/cluster/write?array=sessions&key=user123&value={\"user_id\":123,\"role\":\"admin\",\"expires\":1735689600}"
```

**Read session (Service B):**
```bash
curl "http://localhost:8888/cluster/read?array=sessions&key=user123"
# {"value":"{\"user_id\":123,\"role\":\"admin\",\"expires\":1735689600}"}
```

**Watch sessions (Dashboard):**
```bash
wscat -c "ws://localhost:8888/ws?array=sessions&interval=500"
# Real-time session updates
```

### Example 2: Configuration Management

Share configuration across multiple services.

**Set configuration:**
```bash
curl -X POST "http://localhost:8888/cluster/write?array=config&key=db_host&value=db.internal.local"
curl -X POST "http://localhost:8888/cluster/write?array=config&key=cache_ttl&value=3600"
curl -X POST "http://localhost:8888/cluster/write?array=config&key=log_level&value=info"
```

**Get all configuration:**
```bash
curl "http://localhost:8888/cluster/all?array=config"
```

**Watch for configuration changes:**
```bash
wscat -c "ws://localhost:8888/ws?array=config"
```

### Example 3: Feature Flags

Dynamic feature flag management.

```bash
# Enable new feature
curl -X POST "http://localhost:8888/cluster/write?array=features&key=new_dashboard&value=true"

# Check in service
curl "http://localhost:8888/cluster/read?array=features&key=new_dashboard"

# Disable for specific user
curl -X POST "http://localhost:8888/cluster/write?array=feature_overrides&key=user_123&value=new_dashboard:false"
```

### Example 4: Real-time Metrics

Track real-time metrics across cluster.

```bash
# Service 1 reports metrics
curl -X POST "http://localhost:8888/cluster/write?array=metrics&key=service_1_cpu&value=45.2"

# Service 2 reports metrics
curl -X POST "http://localhost:8888/cluster/write?array=metrics&key=service_2_cpu&value=62.1"

# Dashboard watches all metrics
wscat -c "ws://localhost:8888/ws?array=metrics&interval=100"
```

### Example 5: TCP Backend-to-Backend Communication

Service-to-service communication using TCP protocol.

**Sender (Python):**
```python
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('cluster-node', 9001))

# Authenticate
sock.sendall(b'AUTH your-tcp-key-here\n')
sock.recv(1024)

# Send event
sock.sendall(b'SET events payment_processed order_123\n')
sock.recv(1024)

# Close
sock.sendall(b'QUIT\n')
sock.close()
```

**Receiver (polling):**
```bash
while true; do
  nc cluster-node 9001 << EOF
AUTH your-tcp-key-here
GET events
QUIT
EOF
  sleep 1
done
```

### Example 6: Three-Node Cluster Setup (Docker Compose)

```yaml
version: '3.8'

services:
  cluster-node1:
    image: cluster-app:latest
    ports:
      - "8888:8888"
      - "9000:9000"
      - "9001:9001"
    environment:
      PORT: 8888
      CLUSTER_PEERS: cluster-node2:9000,cluster-node3:9000
      CLUSTER_PORT: 9000
      CLUSTER_CIPHER_KEY: q6BZQxonmHjGTLQsHlQCOTZB27r5VPHHhdI6+r5foLo=
      CLUSTER_PEER_SECRET: YKAXTAWbmSQjc+yIW+RBag==
      TCP_PORT: 9001
      TCP_KEY: 1MHgSdYRzoaMTOW1N3HlXg==

  cluster-node2:
    image: cluster-app:latest
    environment:
      PORT: 8888
      CLUSTER_PEERS: cluster-node1:9000,cluster-node3:9000
      CLUSTER_PORT: 9000
      CLUSTER_CIPHER_KEY: q6BZQxonmHjGTLQsHlQCOTZB27r5VPHHhdI6+r5foLo=
      CLUSTER_PEER_SECRET: YKAXTAWbmSQjc+yIW+RBag==
      TCP_PORT: 9001
      TCP_KEY: 1MHgSdYRzoaMTOW1N3HlXg==

  cluster-node3:
    image: cluster-app:latest
    environment:
      PORT: 8888
      CLUSTER_PEERS: cluster-node1:9000,cluster-node2:9000
      CLUSTER_PORT: 9000
      CLUSTER_CIPHER_KEY: q6BZQxonmHjGTLQsHlQCOTZB27r5VPHHhdI6+r5foLo=
      CLUSTER_PEER_SECRET: YKAXTAWbmSQjc+yIW+RBag==
      TCP_PORT: 9001
      TCP_KEY: 1MHgSdYRzoaMTOW1N3HlXg==
```

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────┐
│                 Cluster Application                  │
├─────────────────────────────────────────────────────┤
│                                                       │
│  ┌─────────────────┐  ┌──────────────────┐           │
│  │  HTTP Server    │  │  WebSocket       │           │
│  │  (Port 8888)    │  │  (Same as HTTP)  │           │
│  └────────┬────────┘  └────────┬─────────┘           │
│           │                    │                     │
│           └────────┬───────────┘                     │
│                    │                                 │
│           ┌────────▼────────┐                        │
│           │  ClusterArray   │  ◄─── Memory          │
│           │   (In-Memory)   │       Storage         │
│           └────────┬────────┘                        │
│                    │                                 │
│    ┌───────────────┼───────────────┐               │
│    │               │               │               │
│  ┌─▼──────┐  ┌─────▼──────┐  ┌────▼────┐          │
│  │ TCP    │  │ Cluster    │  │ File    │          │
│  │Protocol│  │ Peer Sync  │  │ Storage │          │
│  │(9001)  │  │ (9000)     │  │ (batched)         │
│  └────────┘  └────────────┘  └─────────┘          │
│                   │                                 │
│              (Encrypted)                            │
│                   │                                 │
│           ┌───────▼────────┐                       │
│           │  Other Nodes   │                       │
│           │  (IP Whitelisted)                      │
│           └────────────────┘                       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Data Flow

**Write Operation:**
1. Client sends write via HTTP/TCP/WebSocket
2. IP whitelist check
3. Data stored in ClusterArray (memory)
4. Change queued for persistence
5. Persisted to file (batched, 150ms coalesce)
6. Peers notified on next sync cycle
7. Response sent to client

**Read Operation:**
1. Client requests data
2. IP whitelist check
3. Data read from ClusterArray (memory)
4. Serialized to JSON
5. Returned to client

**Peer Sync:**
1. Every 30 seconds (periodicPeerSync)
2. Connect to each configured peer
3. Exchange encrypted data (AES-256-GCM)
4. Merge conflicts (latest timestamp wins)
5. Update local storage

### Data Persistence

**Location:** `runtime/YYYY-MM-DD_HH-MM-SS/` directory

**Files:**
- `arrayname.json` - One file per array
- Content: Pretty-printed JSON (safe for manual inspection)

**Strategy:**
- On-demand writes to file (not on every change)
- Batched writes (150ms window)
- Multiple writes to same array coalesced
- Safe for multi-node setups (each node independent file storage)

### Performance Characteristics

- **In-Memory Speed** - Sub-millisecond reads/writes
- **Network Latency** - TCP/HTTP dominated by network RTT
- **Clustering Overhead** - 30-second sync cycle (low overhead)
- **Persistence Batching** - 150ms coalesce window
- **Max Throughput** - Thousands of ops/sec (limited by network/disk)

---

## Troubleshooting

### Port Already in Use

```
[TCP] Error starting TCP server: listen tcp 0.0.0.0:9001: bind: address already in use
```

**Solution:**
- Kill process using port: `lsof -ti:9001 | xargs kill -9`
- Or change port in `.env`: `TCP_PORT=9002`

### Cannot Connect to Peers

```
[CLUSTER] Error starting peer sync server: listen tcp 0.0.0.0:9000: bind: address already in use
```

**Solution:**
- Check `CLUSTER_PEERS` in `.env` - must be accessible
- Verify firewall allows port 9000
- Ensure same `CLUSTER_CIPHER_KEY` and `CLUSTER_PEER_SECRET`

### IP Whitelisting Blocks Connection

```
HTTP: Access denied: IP not whitelisted
```

**Solution:**
- Check your IP: `curl https://checkip.amazonaws.com`
- Add to whitelist: `HTTP_WHITELIST_IPS=0.0.0.0/0,::/0` (development)
- Or specific: `HTTP_WHITELIST_IPS=127.0.0.1,192.168.1.50`

### Data Not Persisting

**Solution:**
- Check `runtime/` directory exists
- Verify disk space: `df -h`
- Restart app to load from last runtime: `./app --lastconfig`

---

## Best Practices

1. **Use CIDR for subnets** - `192.168.0.0/16` not individual IPs
2. **Rotate keys regularly** - Generate new keys and deploy
3. **Monitor sync status** - Check peer authentication logs
4. **Backup data** - Use `./app export` regularly
5. **Use consistent cluster keys** - All nodes must match
6. **Enable WebSocket for real-time** - Better than polling
7. **Set appropriate update intervals** - Balance responsiveness vs. CPU
8. **Monitor file storage** - Check `runtime/` disk usage
9. **Use --lastconfig on reboot** - Recover last state
10. **Test failover** - Stop nodes to verify redundancy

# tcpclusterd
