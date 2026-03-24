# tcpclusterd - Distributed SQL Database

En distribuert SQL-lignende database med innebygd klyngereplikering, autentisering og sanntidsdatasynkronisering. Støtter både offentlige og private tabeller i samme database.

---

## Rask Start

### 1. Bygg Applikasjonen

```bash
cd /path/to/tcpclusterd
go build -o app
```

### 2. Konfigurer (.env fil)

```bash
# Enkeltserver
CLUSTER_PEERS=""

# Eller flerserver-klynge
CLUSTER_PEERS="[fd00:250:250:d:1::aaaa]:5000,[fd00:250:250:d:1::aaab]:5000"

# Andre innstillinger
ADMIN_PASSWORD="ditt-sikre-passord"
AES_KEY="32-tegns-krypteringsnøkkel-her"
REPLICATION_TOKEN="hemmelig-replikeringstoken"
TOKEN_EXPIRATION="3600"
MAX_TOKENS_PER_USER="10"
```

`MAX_TOKENS_PER_USER` er valgfri. Når en bruker logger inn og går over grensen, slettes den eldste aktive tokenen automatisk.

### 3. Start Serveren

```bash
./app
```

Serveren starter med Unix socket på `/tmp/tcpclusterd.sock` for CLI-kommandoer.

**VIKTIG**: HTTP/TCP/WebSocket-tjenester må konfigureres via CLI for å være tilgjengelige. Se "Service Configuration" nedenfor.

---

## Service Configuration

Serveren starter i "socket-only" modus. For å aktivere HTTP API:

```bash
# Aktiver HTTP service på port 9090
./app --config-http "enabled=true port=9090 host=0.0.0.0"

# Sjekk service status
./app --list-services
```

For WebSocket:
```bash
./app --config-ws "enabled=true port=8080 host=0.0.0.0"
```

---

## CLI-Kommandoer

Alle kommandoer kommuniserer med kjørende server via Unix socket.

### Brukeradministrasjon

```bash
# Legg til ny bruker
./app --add brukernavn --password passord123

# List alle brukere
./app --listusers

# Fjern bruker
./app --remove brukernavn

# Tøm alle autentiseringstokens
./app --flushtokens
```

### Databaseoperasjoner

```bash
# Eksporter alle data til JSON
./app --export

# Eksporter spesifikk database
./app --export mindb

# Importer data fra JSON-fil
./app --import data.json

# Importer og slett eksisterende data først
./app --import data.json --clear
```

### Klyngestyring

```bash
# Vis konfigurerte klyngenoder
./app --list-cluster

# Vis peer-metrikker
./app --peer-metrics

# Vis metrikker for spesifikk peer
./app --peer-metrics "[fd00:250:250:d:1::aaaa]:5000"
```

### Runtime-håndtering

```bash
# Last database fra siste runtime snapshot
./app --lastruntime

# Eksporter fra siste runtime og avslutt
./app --exportlastruntime

# Last fra persistent backup
./app --importpersistent

# Slett gamle runtime-mapper
./app --clearruntimes
```

---

## HTTP API Endepunkter

Alle endepunkter krever autentisering. Serveren lytter på konfigurert port (standard: 9090).

### 1. Autentisering

**POST /api/auth**

**Forespørsel**:
```bash
curl -X POST http://localhost:9090/api/auth \
  -H "Content-Type: application/json" \
  -d '{
    "username": "brukernavn",
    "password": "passord123"
  }'
```

**Svar (Suksess)**:
```json
{
  "ok": true,
  "token": "eyJhbGc...",
  "expires_in": 3600
}
```

**Brute Force-beskyttelse**:
- Maks 20 forsøk per IP per minutt → HTTP 429
- Maks 8 forsøk per bruker+IP per minutt → HTTP 429
- 5-minutters blokkering ved overskridelse

---

### 2. Spørringer med SQL

**POST /api/query** eller **POST /api/execute**

Endepunktet aksepterer SQL-lignende spørringer i JSON-format.

#### SELECT - Hent data

```bash
curl -X POST http://localhost:9090/api/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM mindb.public_tabell WHERE status = '\''online'\''"
  }'
```

**Med PRIVATE scope** (kun admin):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "query": "SELECT PRIVATE * FROM mindb.private_tabell WHERE id = '\''123'\''"
  }'
```

#### INSERT - Sett inn data

```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT PUBLIC INTO mindb.agents (agent_id, name, status) VALUES ('\''agent_001'\'', '\''John Smith'\'', '\''online'\'')"
  }'
```

**INSERT PRIVATE** (kun admin):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "query": "INSERT PRIVATE INTO mindb.credentials (agent_id, phone) VALUES ('\''agent_001'\'', '\''+47-555-1234'\'')"
  }'
```

#### UPDATE - Oppdater data

```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "UPDATE PUBLIC mindb.agents SET status = '\''offline'\'' WHERE agent_id = '\''agent_001'\''"
  }'
```

#### DELETE - Slett data

```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "DELETE PUBLIC FROM mindb.agents WHERE agent_id = '\''agent_001'\''"
  }'
```

#### UPSERT - Sett inn eller oppdater

```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "UPSERT PUBLIC INTO mindb.agents (agent_id, status) VALUES ('\''agent_001'\'', '\''online'\'')"
  }'
```

**Merk**: UPSERT oppdaterer rad hvis den finnes, ellers setter den inn ny rad.

---

### 3. JSON Import (uten SQL-escaping)

**POST /api/import**

For direkte JSON-import uten SQL-formatering:

```bash
curl -X POST http://localhost:9090/api/import \
  -H "Content-Type: application/json" \
  -d '{
    "username": "brukernavn",
    "password": "passord",
    "database": "mindb",
    "table": "agents",
    "private": false,
    "data": [
      {"agent_id": "001", "name": "John", "status": "online"},
      {"agent_id": "002", "name": "Jane", "status": "offline"}
    ]
  }'
```

**Data-felt** kan være:
- Et enkelt objekt: `{"id": "1", "name": "Test"}`
- En array av objekter: `[{...}, {...}]`
- En JSON-streng: `"[{\"id\":\"1\"}]"`

---

### 4. Database Status

**GET /api/status**

```bash
curl http://localhost:9090/api/status
```

**Svar**:
```json
{
  "ok": true,
  "databases": ["system", "mindb"],
  "peers": ["[fd00:250:250:d:1::aaaa]:5000"]
}
```

---

### 5. Offentlig Database-data

**GET /api/databases/{database}**

Henter alle offentlige tabeller for en database:

```bash
curl http://localhost:9090/api/databases/mindb
```

---

### 6. Health Check

**GET /health**

```bash
curl http://localhost:9090/health
```

**Svar**:
```json
{
  "ok": true,
  "status": "healthy"
}
```

---

## SQL-Syntaks

### Støttede kommandoer

- **SELECT** - Hent data fra tabell
- **INSERT** - Sett inn ny rad
- **UPDATE** - Oppdater eksisterende rader
- **DELETE** - Slett rader
- **UPSERT** - Sett inn eller oppdater

### Scope (PUBLIC/PRIVATE)

```sql
-- PUBLIC (standard, alle autentiserte brukere)
INSERT PUBLIC INTO db.table (col1, col2) VALUES ('val1', 'val2')
SELECT * FROM db.table WHERE col = 'value'

-- PRIVATE (kun admin)
INSERT PRIVATE INTO db.table (secret) VALUES ('hemmelig')
SELECT PRIVATE * FROM db.table WHERE id = '123'
```

Hvis `PUBLIC`/`PRIVATE` ikke spesifiseres, brukes `PRIVATE` som standard.

### WHERE-betingelser

```sql
SELECT * FROM db.table WHERE status = 'online'
SELECT * FROM db.table WHERE id = '123' AND active = 'true'
DELETE FROM db.table WHERE timestamp < '1672531200'
```

---

## Systemtabeller

### system.users
Brukerkontoer og legitimasjon (privat).

```json
{
  "username": "john",
  "password_hash": "...",
  "is_admin": false,
  "created_at": 1772814834,
  "updated_at": 1772815881
}
```

### system.tokens
Autentiseringstokens med utløp (privat).

```json
{
  "token": "eyJhbGc...",
  "username": "john",
  "issued_at": 1772814834,
  "expires_at": 1772818434,
  "is_consumed": false
}
```

### system.auth_rate_limits
Sporings av mislykkede autentiseringsforsøk (distribuert).

```json
{
  "scope": "ip",
  "key": "192.168.1.100",
  "attempts": 5,
  "window_start": 1772814834,
  "blocked_until": 1772815134
}
```

### system.peer_metrics
Klyngenodestatus og replikeringsstatistikk (offentlig).

```json
{
  "peer_address": "[fd00:250:250:d:1::aaaa]:5000",
  "is_online": true,
  "last_ping": 1772824145,
  "replication_failures": 0,
  "replication_lag_ms": 45,
  "writes_replicated": 1523
}
```

### system.services_config
Tjenesteinnstillinger (HTTP/TCP/WebSocket).

```json
{
  "service": "http",
  "enabled": true,
  "host": "0.0.0.0",
  "port": 9090,
  "tls": false
}
```

---

## Brukseksempel: Agent Status-system

### Problem
En kundeservice-nettside som viser agentstatus:
- **Offentlige data**: Agent ID, Navn, Status (Online/Offline)
- **Private data**: Telefonnummer, E-post, Passord

### Løsning

**Opprett offentlig tabell** (alle kan lese):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT PUBLIC INTO agents.status (agent_id, name, status) VALUES ('\''001'\'', '\''John'\'', '\''online'\'')"
  }'
```

**Opprett privat tabell** (kun admin):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "query": "INSERT PRIVATE INTO agents.credentials (agent_id, phone, email) VALUES ('\''001'\'', '\''+47-555-1234'\'', '\''john@example.com'\'')"
  }'
```

**Kunder henter status** (offentlig):
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $CUSTOMER_TOKEN" \
  -d '{
    "query": "SELECT * FROM agents.status WHERE status = '\''online'\''"
  }'
# Returnerer: agent_id, name, status (ikke telefon/e-post)
```

**Admin henter private data**:
```bash
curl -X POST http://localhost:9090/api/query \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "query": "SELECT PRIVATE * FROM agents.credentials WHERE agent_id = '\''001'\''"
  }'
# Returnerer: agent_id, phone, email
```

---

## Klyngeoppsett

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


### Flerserver-oppsett

**Node 1** (`[fd00:250:250:d:1::aaaa]:5000`):
```bash
# .env
CLUSTER_PEERS="[fd00:250:250:d:1::aaaa]:5000,[fd00:250:250:d:1::aaab]:5000"

# Start server
./app

# Aktiver HTTP service
./app --config-http "enabled=true port=9090 host=0.0.0.0"
```

**Node 2** (`[fd00:250:250:d:1::aaab]:5000`):
```bash
# .env
CLUSTER_PEERS="[fd00:250:250:d:1::aaaa]:5000,[fd00:250:250:d:1::aaab]:5000"

# Start server
./app

# Aktiver HTTP service
./app --config-http "enabled=true port=9090 host=0.0.0.0"
```

**Automatisk oppførsel**:
- Alle skriveoperasjoner replikeres til alle noder
- Hvis en node er nede, fungerer spørringer fortsatt (eventual consistency)
- Når noden kommer tilbake, synkroniserer den automatisk
- Replikering er idempotent (ingen duplikate data)

### Sjekk klyngestatus

```bash
./app --peer-metrics

# Viser:
# - Hvilke peers som er online
# - Siste ping-tid
# - Replikeringsforsinkelse
# - Antall replikerte skriveoperasjoner
```

---

## Sikkerhetsfunksjoner

### 1. Autentisering
- Brukernavn/passord med bcrypt-hashing
- Engangstokens (JWT med AES-kryptering)
- Token TTL (konfigurerbar via `TOKEN_TTL_SECONDS`)
- Automatisk token-opprydding (utløpte tokens slettes)

### 2. Brute Force-beskyttelse
- Per-IP begrensning: maks 20 forsøk/minutt
- Per-bruker+IP begrensning: maks 8 forsøk/minutt
- Automatisk 5-minutters blokkering ved overskridelse
- Rate limit-tilstand replikert på tvers av klyngen

### 3. Replikeringssikkerhet
- Replikeringstoken-autentisering (`X-Replication-Token` header)
- Forespørselsgrenser (1 MiB per forespørsel)
- Idempotente skriveoperasjoner (forhindrer duplikatanvendelse)
- Constant-time sammenligning for tokens

### 4. Dataprivacy
- Private tabeller skjult fra ikke-admin spørringer
- Kun admin-tilgang til sensitive data
- Separat autentisering per bruker

---

## Feilsøking

### Import timeout ved nettverksproblemer

```
[ERROR] import failed: cluster write timed out - check if all nodes are online 
and network connectivity is available. I did a ping test to 8.8.8.8 (OK) and 
[2001:4860:4860::8888] (TIMEOUT)
```

**Løsning**: Sjekk nettverkstilkobling. Hvis IPv6 er nede, sørg for at alle noder er IPv4-tilgjengelige, eller konfigurer IPv6 korrekt.

### Peer-metrikker viser duplikater eller ephemeral porter

**Årsak**: Innkommende tilkoblinger oppretter midlertidige oppføringer med dynamiske porter.

**Løsning**: Viste peers filtreres automatisk til kun konfigurerte peers i `CLUSTER_PEERS`. Ephemeral oppføringer skjules.

### Treg replikering

```bash
./app --peer-metrics
```

Sjekk `replication_lag_ms`. Hvis høy (>500ms):
- Sjekk nettverksbåndbredde
- Sjekk disk I/O på målnoder
- Sørg for at alle noder er friske

### HTTP service ikke tilgjengelig

**Problem**: `curl: (7) Failed to connect to localhost port 9090`

**Løsning**: HTTP service må aktiveres eksplisitt:
```bash
./app --config-http "enabled=true port=9090 host=0.0.0.0"

# Verifiser
./app --list-services
```

---

## Ytelse

- **Enkeltnode**: ~10,000 spørringer/sekund
- **Klyngeskriving**: 5-10 sekunders timeout per skriving (konfigurerbar)
- **Replikering**: Sub-sekund til peers på samme nettverk
- **Minne**: ~100 MB base + datastørrelse

---

## Backup og Gjenoppretting

### Automatiske backups

Backups opprettes i `backups/`-mappen:
```bash
ls backups/
backup_2026-03-06_19-23-45.json
backup_2026-03-06_19-13-45.json
```

### Manuell backup

```bash
./app --export > backup_manual.json
```

### Gjenopprett fra backup

```bash
./app --import backup_manual.json --clear
```

---

## Konfigurasjonsreferanse

| Variabel | Standard | Beskrivelse |
|----------|---------|-------------|
| `CLUSTER_PEERS` | "" | Kommaseparerte klynge peer-adresser |
| `ADMIN_PASSWORD` | "admin" | Admin-brukerpassord |
| `RUNTIME_PATH` | "runtime" | Katalog for runtime snapshots |
| `PERSISTENT_BACKUP_PATH` | "backups" | Katalog for backups |
| `LOG_LEVEL` | "info" | Loggingsnivå (debug, info, warn, error) |
| `REPLICATION_TOKEN` | "" | Hemmelig token for klyngereplikering |
| `TOKEN_TTL_SECONDS` | "" | Auth token-utløp (tom = ingen utløp) |
| `AES_KEY` | "default-secret..." | Krypteringsnøkkel for tokens (32 tegn) |
| `AUTO_BACKUP` | "true" | Aktiver automatiske backups |
| `AUTO_CLEAR_RUNTIMES` | "false" | Auto-slett gamle runtime snapshots |

---

## Oppsummering

tcpclusterd tilbyr:

✓ **Offentlige + Private data** i samme database  
✓ **Sanntids klyngereplikering** på tvers av flere noder  
✓ **Autentisering og autorisasjon** med rate limiting  
✓ **Sikkerhet** med token-kryptering og brute-force beskyttelse  
✓ **Automatisk failover** når noder går ned  
✓ **Enkel REST API** med SQL-lignende spørrespråk  

Perfekt for systemer som trenger å eksponere noe data (agentstatus) mens andre data beskyttes (telefonnumre, passord, interne IDer).
