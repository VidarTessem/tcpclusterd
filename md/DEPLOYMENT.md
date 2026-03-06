# tcpclusterd Deployment Guide

## Pre-Deployment Checklist

- [ ] Go 1.22 or later installed
- [ ] Network connectivity between cluster nodes verified
- [ ] Firewall rules allowing cluster port (default 5000) between nodes
- [ ] HTTP service port (default 9090) accessible to clients
- [ ] Backup storage location available
- [ ] TLS/HTTPS configured externally (reverse proxy recommended)

---

## Single Server Deployment

### 1. Prepare the Application

```bash
cd /path/to/tcpclusterd
go build -o app
chmod +x app
```

### 2. Configure `.env`

```bash
cp .env.example .env

# Edit .env - minimal for single server:
# CLUSTER_PEERS=              (leave empty for single server)
# ADMIN_PASSWORD=YourSecure123
# AES_KEY=generateWithOpenSSL (see below)
# REPLICATION_TOKEN=          (not needed for single server)
```

Generate secure keys:
```bash
# Generate AES key
openssl rand -base64 32

# This is your REPLICATION_TOKEN (for future multi-server setup)
openssl rand -base64 32
```

### 3. Start the Server

```bash
./app
```

Server starts on port 9090 by default. Output shows:
```
[INFO] Server started
[INFO] Socket server listening on /tmp/tcpclusterd.sock
[INFO] Databases: [system]
```

### 4. Test Connectivity

```bash
# In another terminal, authenticate
TOKEN=$(curl -s -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"YourSecure123"}' \
  | jq -r '.token')

# Query system data
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database":"system","table":"users","where":{}}'
```

---

## Multi-Server Cluster Deployment

### 1. Plan Your Cluster

Decide on:
- **Number of nodes**: 2, 3, 5 (odd numbers recommended)
- **Peer addresses**: IPv4 or IPv6, all with port 5000
- **Replication token**: Shared secret for all nodes

Example: 3-node cluster
```
Node 1: 192.168.1.100:5000
Node 2: 192.168.1.101:5000
Node 3: 192.168.1.102:5000
```

Or IPv6:
```
Node 1: [2a03:94e0:205b:d:1::aaaa]:5000
Node 2: [2a03:94e0:205b:d:1::aaab]:5000
Node 3: [2a03:94e0:205b:d:1::aaac]:5000
```

### 2. Build on All Nodes

```bash
go build -o app
chmod +x app
```

### 3. Configure Each Node

**Node 1** (example IPv6):
```bash
cp .env.example .env

# Edit .env
CLUSTER_PEERS="[2a03:94e0:205b:d:1::aaaa]:5000,[2a03:94e0:205b:d:1::aaab]:5000,[2a03:94e0:205b:d:1::aaac]:5000"
CLUSTER_DEFAULT_PORT=5000
ADMIN_PASSWORD=SecureAdminPassword123
REPLICATION_TOKEN=SharedTokenFor3Nodes...
AES_KEY=YourGeneratedAESKey...
LOG_LEVEL=info
AUTO_BACKUP=true
TOKEN_TTL_SECONDS=3600
```

**Repeat for Node 2 and 3** with same `CLUSTER_PEERS` and `REPLICATION_TOKEN`.

### 4. Start All Nodes

**Terminal on Node 1:**
```bash
./app
# [INFO] Cluster node at [2a03:94e0:205b:d:1::aaaa]:5000
# [INFO] Peer server started for cluster communication
```

**Terminal on Node 2:**
```bash
./app
# [INFO] Cluster node at [2a03:94e0:205b:d:1::aaab]:5000
# [INFO] Peer server started for cluster communication
```

**Terminal on Node 3:**
```bash
./app
# [INFO] Cluster node at [2a03:94e0:205b:d:1::aaac]:5000
# [INFO] Peer server started for cluster communication
```

### 5. Verify Cluster Health

From any node:
```bash
./app --peer-metrics

# Output shows all nodes and their status:
{
  "data": [
    {
      "peer_address": "[2a03:94e0:205b:d:1::aaaa]:5000",
      "is_online": true,
      "last_ping": 1772824145
    },
    {
      "peer_address": "[2a03:94e0:205b:d:1::aaab]:5000",
      "is_online": true,
      "last_ping": 1772824142
    },
    {
      "peer_address": "[2a03:94e0:205b:d:1::aaac]:5000",
      "is_online": true,
      "last_ping": 1772824140
    }
  ]
}
```

### 6. Test Cluster Replication

Create data on Node 1:
```bash
TOKEN=$(curl -s -X POST http://node1:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"SecureAdminPassword123"}' \
  | jq -r '.token')

curl -X POST http://node1:9090/api/write \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database": "test",
    "table": "data",
    "operation": "insert",
    "data": {"id": "001", "value": "replicated"}
  }'
```

Query from Node 2 to verify replication:
```bash
TOKEN=$(curl -s -X POST http://node2:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"SecureAdminPassword123"}' \
  | jq -r '.token')

curl -X POST http://node2:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database":"test","table":"data","where":{}}'

# Should return the data created on Node 1
```

---

## Production Best Practices

### 1. Use Reverse Proxy (Nginx Example)

```nginx
upstream tcpclusterd {
  server 192.168.1.100:9090;
  server 192.168.1.101:9090;
  server 192.168.1.102:9090;
}

server {
  listen 443 ssl http2;
  server_name api.example.com;

  ssl_certificate /path/to/cert.pem;
  ssl_certificate_key /path/to/key.pem;

  location /api/ {
    proxy_pass http://tcpclusterd;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
  }
}
```

### 2. Process Management (Systemd Example)

Create `/etc/systemd/system/tcpclusterd.service`:
```ini
[Unit]
Description=tcpclusterd Cluster Database
After=network.target

[Service]
Type=simple
User=tcpclusterd
WorkingDirectory=/opt/tcpclusterd
EnvironmentFile=/opt/tcpclusterd/.env
ExecStart=/opt/tcpclusterd/app
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable tcpclusterd
sudo systemctl start tcpclusterd
sudo systemctl status tcpclusterd
```

### 3. Backup Strategy

Automatic backups created in `backups/` directory. Set up external backup:

```bash
# Daily backup to external storage
0 2 * * * cp /opt/tcpclusterd/backups/* /backup/tcpclusterd/ 2>/dev/null
```

### 4. Monitoring

Check logs:
```bash
tail -f logs/app.log
tail -f logs/audit.log
```

Monitor cluster health periodically:
```bash
# Every 5 minutes
*/5 * * * * /opt/tcpclusterd/app --peer-metrics > /tmp/cluster_status.json
```

### 5. Security Hardening

**Firewall rules** (iptables example):
```bash
# Allow cluster replication only between nodes
sudo iptables -A INPUT -p tcp --dport 5000 -s 192.168.1.0/24 -j ACCEPT
sudo iptables -A INPUT -p tcp --dport 5000 -j DROP

# Allow HTTPS to API from anywhere
sudo iptables -A INPUT -p tcp --dport 443 -j ACCEPT

# Allow SSH for management
sudo iptables -A INPUT -p tcp --dport 22 -s 192.168.1.0/24 -j ACCEPT
```

**TLS/HTTPS**: Always use HTTPS in production (configure via reverse proxy).

**Strong passwords**: Change `ADMIN_PASSWORD` from default.

**Token rotation**: Set `TOKEN_TTL_SECONDS` to rotate tokens (e.g., 3600 = 1 hour).

**Rate limiting**: Built-in brute-force protection (20 attempts/IP/minute).

---

## Troubleshooting

### Cannot connect to cluster

```
failed to replicate to peer 192.168.1.101:5000
```

**Check**:
1. All nodes are running: `systemctl status tcpclusterd`
2. Network connectivity: `ping 192.168.1.101`
3. Firewall allows port 5000: `telnet 192.168.1.101 5000`
4. Same `REPLICATION_TOKEN` on all nodes
5. Same `CLUSTER_PEERS` on all nodes

### Token TTL cleanup not working

Make sure `TOKEN_TTL_SECONDS` is set and > 0:
```bash
grep TOKEN_TTL_SECONDS .env
```

If not set, tokens never expire (set to 3600 for 1 hour).

### Replication lag is high

```bash
./app --peer-metrics
# Check replication_lag_ms

# If > 500ms:
# - Network latency issue
# - Disk I/O bottleneck
# - Node is overloaded
```

### Node went down, recovery

1. Fix the underlying issue
2. Start the node: `systemctl start tcpclusterd`
3. Verify it syncs: `./app --peer-metrics`
4. Check logs: `tail logs/app.log`

---

## Scaling Considerations

### Single Server
- Fine for < 10,000 QPS
- Easy to manage
- No replication overhead

### 3-Server Cluster
- Good balance of fault tolerance + complexity
- Can handle 1 node failure
- ~10-50 ms replication latency

### 5+ Server Cluster
- High availability setup
- Can handle 2+ node failures
- Consider read replicas

---

## Example: Complete Cluster Setup (3 Nodes)

```bash
# Node 1: 192.168.1.100
cat > /opt/tcpclusterd/.env << EOF
CLUSTER_PEERS=192.168.1.100:5000,192.168.1.101:5000,192.168.1.102:5000
ADMIN_PASSWORD=$(openssl rand -base64 16)
REPLICATION_TOKEN=$(openssl rand -base64 32)
AES_KEY=$(openssl rand -base64 32)
LOG_LEVEL=info
TOKEN_TTL_SECONDS=3600
EOF

# Copy to Node 2 and Node 3
scp /opt/tcpclusterd/.env node2:/opt/tcpclusterd/
scp /opt/tcpclusterd/.env node3:/opt/tcpclusterd/

# Start all nodes
ssh node1 "systemctl start tcpclusterd"
ssh node2 "systemctl start tcpclusterd"
ssh node3 "systemctl start tcpclusterd"

# Verify
./app --peer-metrics
```

Done! Your cluster is ready for production.
