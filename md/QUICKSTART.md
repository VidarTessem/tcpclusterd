# Quick Start - 5 Minutes

## Build

```bash
go build -o app
```

## Configure

```bash
cp .env.example .env
# Edit .env and set: ADMIN_PASSWORD, AES_KEY
```

## Start

```bash
./app
```

## Test (in another terminal)

```bash
# Get token
TOKEN=$(curl -s -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"YOUR_PASSWORD"}' \
  | jq -r '.token')

# Query
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database":"system","table":"users","where":{}}'

# Insert
curl -X POST http://localhost:9090/api/write \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "database":"mydb",
    "table":"public_data",
    "operation":"insert",
    "data":{"id":"1","name":"John","status":"online"}
  }'

# Query what you inserted
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"database":"mydb","table":"public_data","where":{}}'
```

## CLI Commands

```bash
# Add user
./app --add username --password pass123

# List users
./app --listusers

# Export data
./app --export > backup.json

# Import data
./app --import backup.json

# Import (clear first)
./app --import backup.json --clear

# Cluster status
./app --peer-metrics
```

---

## Key Points

- **Public Tables**: Shown to anyone with auth token
- **Private Tables**: Only admin can see (add `is_private: true`)
- **Cluster**: Set `CLUSTER_PEERS` in `.env` for multi-server
- **Security**: Change `ADMIN_PASSWORD` from default!

---

## For Complete Docs

See:
- **README_PRODUCTION.md** - Full API, examples, use cases
- **DEPLOYMENT.md** - Production setup, monitoring, scaling
