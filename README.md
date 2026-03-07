# tcpclusterd - Production Cluster Database

A distributed SQL-like database for managing **public and private data** simultaneously, with built-in cluster replication, authentication, and real-time data synchronization.

## Use Cases

### Primary Use Case: Agent Status System
Imagine a **customer-facing website** where agents handle phone calls:
- **Public Data** (show to website visitors): Agent ID, Agent Name, Status (Online/Offline), Last Updated
- **Private Data** (never exposed): Agent's Real Phone Number, Password, Internal ID, Email

**Problem**: You need to show agent availability in real-time to customers without exposing sensitive information.

**Solution**: tcpclusterd stores all data in one database but exports only public fields via API.

**Example**:
```json
// Stored internally (private access)
{
  "agent_id": "agent_001",
  "agent_name": "John Smith",
  "status": "online",
  "phone_number": "+47-555-1234",     // PRIVATE
  "email": "john@company.no",          // PRIVATE
  "real_phone": "93949555"             // PRIVATE
}

// When customer queries /api/query for PUBLIC table
// They only see:
{
  "agent_id": "agent_001",
  "agent_name": "John Smith",
  "status": "online"
}
```

---

## Quick Start

### 1. Build the Application

```bash
cd /path/to/tcpclusterd
go build -o app
```

### 2. Configure Your Cluster

Create a `.env` file:

```bash
# Single Server
CLUSTER_PEERS=""

# Or Multi-Server Cluster
CLUSTER_PEERS="[2a03:94e0:205b:d:1::aaaa]:5000,[2a03:94e0:205b:d:1::aaab]:5000"

# Other settings
ADMIN_PASSWORD="your-secure-password"
RUNTIME_PATH="runtime"
PERSISTENT_BACKUP_PATH="backups"
LOG_LEVEL="info"
REPLICATION_TOKEN="your-secret-replication-token"
TOKEN_TTL_SECONDS="3600"
AES_KEY="your-aes-encryption-key-32-chars"
```

### 3. Start the Server

```bash
./app
```

The server starts a Unix socket at `/tmp/tcpclusterd.sock` for CLI commands.

---

## CLI Commands

All commands communicate with the running server via Unix socket. **The server must be running in another terminal.**

### User Management

```bash
# Add a new user (creates personal database)
./app --add john --password securepass123

# List all users
./app --listusers

# Flush all authentication tokens
./app --flushtokens
```

### Database Operations

```bash
# Export all data to JSON
./app --export

# Export specific database
./app --export mydb

# Import data from JSON file (append mode)
./app --import data.json

# Import and clear existing data first (replace mode)
./app --import data.json --clear
```

### Cluster Management

```bash
# Show all configured cluster peers
./app --list-cluster

# Show peer connectivity metrics
./app --peer-metrics

# Show metrics for specific peer
./app --peer-metrics "[2a03:94e0:205b:d:1::aaaa]:5000"
```

### Runtime Management

```bash
# Load database from last runtime snapshot
./app --lastruntime

# Export from last runtime and exit
./app --exportlastruntime

# Load from persistent backup
./app --importpersistent

# Clear old runtime folders
./app --clearruntimes
```

---

## HTTP API Endpoints

All endpoints require authentication. The server listens on the port configured for HTTP service (default: 9090).

### 1. Authentication

**Endpoint**: `POST /api/auth`

**Request**:
```bash
curl -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{
    "username": "john",
    "password": "securepass123"
  }'
```

**Response** (Success):
```json
{
  "ok": true,
  "token": "eyJhbGc...",
  "expires_in": 3600
}
```

**Response** (Failure):
```json
{
  "ok": false,
  "error": "invalid credentials"
}
```

**Brute Force Protection**:
- Max 20 attempts per IP per minute → HTTP 429
- Max 8 attempts per user+IP per minute → HTTP 429
- 5-minute temporary block on exceeded limits

---

### 2. Query Data

**Endpoint**: `POST /api/query`

**Request** (Query PUBLIC data):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_status",
    "where": {}
  }'
```

**Response**:
```json
{
  "ok": true,
  "data": [
    {
      "agent_id": "agent_001",
      "agent_name": "John Smith",
      "status": "online"
    },
    {
      "agent_id": "agent_002",
      "agent_name": "Jane Doe",
      "status": "offline"
    }
  ]
}
```

**Query with Filters**:
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_status",
    "where": {
      "status": "online"
    }
  }'
```

**Query PRIVATE data** (admin only):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_credentials",
    "is_private": true,
    "where": {
      "agent_id": "agent_001"
    }
  }'
```

---

### 3. Insert Data

**Endpoint**: `POST /api/write`

**Insert PUBLIC data**:
```bash
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_status",
    "operation": "insert",
    "data": {
      "agent_id": "agent_003",
      "agent_name": "Bob Johnson",
      "status": "online"
    }
  }'
```

**Insert PRIVATE data** (admin only):
```bash
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_credentials",
    "operation": "insert",
    "is_private": true,
    "data": {
      "agent_id": "agent_003",
      "phone_number": "+47-555-9999",
      "real_phone": "91234567"
    }
  }'
```

---

### 4. Update Data

**Endpoint**: `POST /api/write`

**Update agent status**:
```bash
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_status",
    "operation": "update",
    "data": {
      "status": "offline",
      "updated_at": 1772824145
    },
    "where": {
      "agent_id": "agent_001"
    }
  }'
```

---

### 5. Delete Data

**Endpoint**: `POST /api/write`

**Delete agent record**:
```bash
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "database": "agents",
    "table": "agent_status",
    "operation": "delete",
    "where": {
      "agent_id": "agent_001"
    }
  }'
```

---

## Database Schema

### System Tables (Automatic)

#### `system.users`
Stores user accounts and credentials (private).

```json
{
  "username": "john",
  "password_hash": "...",
  "is_admin": false,
  "created_at": 1772814834,
  "updated_at": 1772815881
}
```

#### `system.tokens`
Stores authentication tokens with expiration (private).

```json
{
  "token": "eyJhbGc...",
  "username": "john",
  "issued_at": 1772814834,
  "expires_at": 1772818434,
  "is_consumed": false
}
```

#### `system.auth_rate_limits`
Tracks failed authentication attempts (distributed across cluster).

```json
{
  "scope": "ip",
  "key": "192.168.1.100",
  "attempts": 5,
  "window_start": 1772814834,
  "blocked_until": 1772815134
}
```

#### `system.peer_metrics`
Cluster node status and replication statistics (public).

```json
{
  "peer_address": "[2a03:94e0:205b:d:1::aaaa]:5000",
  "is_online": true,
  "last_ping": 1772824145,
  "replication_failures": 0,
  "replication_lag_ms": 45,
  "writes_replicated": 1523,
  "created_at": 1772814834,
  "updated_at": 1772824145
}
```

---

## Real-World Example: Agent Status Website

### Scenario
You're running a call center and need a website where customers can see if agents are available.

### Setup

**1. Create the agent database:**

```bash
./app --add admin --password admin123
```

**2. Initialize data via API:**

```bash
# Get authentication token
TOKEN=$(curl -s -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  | jq -r '.token')

# Create PUBLIC table with agent status
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "call_center",
    "table": "agent_status",
    "operation": "insert",
    "data": {
      "agent_id": "agent_001",
      "agent_name": "John Smith",
      "status": "online",
      "updated_at": '$(date +%s)'
    }
  }'

# Create PRIVATE table with real phone numbers
curl -X POST http://localhost:9090/api/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "call_center",
    "table": "agent_credentials",
    "operation": "insert",
    "is_private": true,
    "data": {
      "agent_id": "agent_001",
      "phone_number": "+47-555-1234",
      "email": "john@company.no"
    }
  }'
```

**3. Customer website queries PUBLIC data:**

```bash
# Customer app gets token
TOKEN=$(curl -s -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"website","password":"public_password"}' \
  | jq -r '.token')

# Query - only gets public fields!
curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "database": "call_center",
    "table": "agent_status",
    "where": {}
  }'

# Result shows only:
# agent_id, agent_name, status
# NO phone numbers!
```

**4. Admin queries PRIVATE data:**

```bash
ADMIN_TOKEN=$(curl -s -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' \
  | jq -r '.token')

curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "database": "call_center",
    "table": "agent_credentials",
    "is_private": true,
    "where": {"agent_id": "agent_001"}
  }'

# Result shows everything (admin only):
# agent_id, phone_number, email
```

---

## Cluster Deployment

### Multi-Server Setup

**Node 1** (`[2a03:94e0:205b:d:1::aaaa]:5000`):
```bash
CLUSTER_PEERS="[2a03:94e0:205b:d:1::aaaa]:5000,[2a03:94e0:205b:d:1::aaab]:5000"
./app
```

**Node 2** (`[2a03:94e0:205b:d:1::aaab]:5000`):
```bash
CLUSTER_PEERS="[2a03:94e0:205b:d:1::aaaa]:5000,[2a03:94e0:205b:d:1::aaab]:5000"
./app
```

**Automatic behavior**:
- All writes are replicated to all nodes
- If one node is down, queries still work (eventual consistency)
- When node comes back online, it syncs automatically
- Replication is idempotent (no duplicate data)

### Check Cluster Status

```bash
./app --peer-metrics

# Output shows:
# - Which peers are online
# - Last ping time
# - Replication lag
# - Number of replicated writes
```

---

## Security Features

### 1. Authentication
- Username/password with bcrypt hashing
- One-time tokens (JWT with AES encryption)
- Token TTL (configurable via `TOKEN_TTL_SECONDS`)
- Automatic token cleanup (expired tokens deleted)

### 2. Brute Force Protection
- Per-IP rate limiting: max 20 attempts/minute
- Per-user+IP rate limiting: max 8 attempts/minute
- Automatic 5-minute block on exceeded limits
- Rate limit state replicated across cluster

### 3. Replication Security
- Replication token authentication (`X-Replication-Token` header)
- Request body size limits (1 MiB per request)
- Idempotent writes (prevents duplicate applies)
- Constant-time comparison for tokens

### 4. Data Privacy
- Private tables hidden from non-admin queries
- Admin-only access to sensitive data
- Separate authentication per user

---

## Troubleshooting

### Import times out during network issues

```
[ERROR] import failed: cluster write timed out - check if all nodes are online 
and network connectivity is available. I did a ping test to 8.8.8.8 (OK) and 
[2001:4860:4860::8888] (TIMEOUT)
```

**Solution**: Check network connectivity. If IPv6 is down, ensure all nodes are IPv4 accessible, or configure IPv6 properly.

### Peer metrics show duplicates or ephemeral ports

**Cause**: Inbound connections are creating temporary entries with dynamic ports.

**Solution**: Peers shown are automatically filtered to only configured peers in `CLUSTER_PEERS`. Ephemeral entries are hidden.

### Slow replication

```bash
./app --peer-metrics
```

Check `replication_lag_ms`. If high (>500ms):
- Check network bandwidth
- Check disk I/O on target nodes
- Ensure all nodes are healthy

---

## Performance

- **Single node**: ~10,000 queries/second
- **Cluster write**: 5-10 second timeout per write (configurable)
- **Replication**: Sub-second to peers on same network
- **Memory**: ~100 MB base + data size

---

## Backup & Recovery

### Automatic Backups

Backups are created in `backups/` folder:
```bash
ls backups/
backup_2026-03-06_19-23-45.json
backup_2026-03-06_19-13-45.json
```

### Manual Backup

```bash
./app --export > backup_manual.json
```

### Restore from Backup

```bash
./app --import backup_manual.json --clear
```

---

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `CLUSTER_PEERS` | "" | Comma-separated cluster peer addresses |
| `ADMIN_PASSWORD` | "admin" | Admin user password |
| `RUNTIME_PATH` | "runtime" | Directory for runtime snapshots |
| `PERSISTENT_BACKUP_PATH` | "backups" | Directory for backups |
| `LOG_LEVEL` | "info" | Log verbosity (debug, info, warn, error) |
| `REPLICATION_TOKEN` | "" | Secret token for cluster replication |
| `TOKEN_TTL_SECONDS` | "" | Auth token expiration (empty = no expiration) |
| `AES_KEY` | "default-secret..." | Encryption key for tokens (32 chars) |
| `AUTO_BACKUP` | "true" | Enable automatic backups |
| `AUTO_CLEAR_RUNTIMES` | "false" | Auto-delete old runtime snapshots |

---

## Summary

tcpclusterd provides:
✓ **Public + Private Data** in one database  
✓ **Real-time Cluster Replication** across multiple nodes  
✓ **Authentication & Authorization** with rate limiting  
✓ **Security** with token encryption and brute-force protection  
✓ **Automatic Failover** when nodes go down  
✓ **Simple REST API** for queries and writes  

Perfect for systems that need to expose some data (agent status) while protecting other data (phone numbers, passwords, internal IDs).
