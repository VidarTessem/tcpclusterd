# tcpclusterd - Distribuert Cluster Array Server

## Hva er dette?

`tcpclusterd` er en distribuert server for lagring og synkronisering av data across flere noder i en cluster. Den tilbyr flere protokoller (HTTP/REST, WebSocket, TCP) for å få tilgang til data, med innebygd støtte for:

- **Automatisk peer-synkronisering** - Data synkroniseres automatisk mellom alle noder
- **Kryptering** - AES-256-GCM kryptering for all komunikasjon mellom nodes
- **Autentisering** - Basic Auth for HTTP, TCP nøkkel-basert autentisering
- **Offentlig/private data** - Atskilt lagring av offentlig og privat data med access-kontroll
- **Persistering** - Data lagres lokalt på disk i JSON format
- **Recovery** - Automatisk recovery fra disk eller fra peer nodes ved oppstart
- **Metrics** - Helse-status, opptime, error-tracking, og peer-information

## Hva løser dette?

### Problem 1: Datafordeling across flere servere
**Løsning**: Alle nodes synkroniseres automatisk. Når data skrives til en node, propageres det til alle andre noder i clusteret.

```
Node A receives: POST /cluster/write?array=users
                ↓
Stored locally + sent to peers
                ↓
Node B og Node C får data automatisk
```

### Problem 2: Data sikkerhet ved node-feil
**Løsning**: Data er replikert på flere noder. Hvis en node går ned, er data fortsatt tilgjengelig på de andre nodene. Når noden kommer opp igjen, gjenopprettes data automatisk.

```
Node A (down for 5 min) → Starter opp → Synkroniserer fra Node B/C → Har samme data som før
```

### Problem 3: Høy tilgjengelighet
**Løsning**: Clients kan koble seg til hvilken som helst node. Hvis en node er nede, kobler de seg til en annen.

```
Client: Load balance across nodes
        Node A down → Connect to Node B
        Node B down → Connect to Node C
```

### Problem 4: Sikring av sensitive data
**Løsning**: Private arrays krever autentisering. Offentlige og private data lagres atskilt.

```
GET /cluster/all              → Returnerer bare offentlige arrays
GET /cluster/private/all      → Krever Basic Auth → Returnerer både offentlig og privat data
```

## Hvordan brukes det?

### 1. Oppstart

```bash
# Initialisering (første gang)
./app init

# Vanlig oppstart (synkroniserer kun fra peers)
./app

# Oppstart med recovery fra disk
./app --recover
```

### 2. HTTP REST API

```bash
# Les all offentlig data
curl http://localhost:8080/cluster/all

# Les privat data (krever autentisering)
curl -u admin:passord http://localhost:8080/cluster/private/all

# Skriv til array
curl -X POST http://localhost:8080/cluster/write?array=users \
  -H "Content-Type: application/json" \
  -d '{"user1": "data1"}'

# Les enkelt array
curl http://localhost:8080/cluster/read?array=users

# Slett fra array
curl -X DELETE http://localhost:8080/cluster/delete?array=users&key=user1
```

### 3. WebSocket (Live oppdateringer)

```javascript
// Koble til WebSocket for live data
const ws = new WebSocket('ws://localhost:8080/ws?array=users&interval=1000');
ws.onmessage = (event) => {
  console.log('Data oppdatering:', JSON.parse(event.data));
};
```

### 4. TCP Protocol (Direkte cluster-kommunikasjon)

```bash
# TCP autentisering + GET_ARRAYS
echo -e "your-tcp-key\nGET_ARRAYS" | nc localhost:9000
```

### 5. Export/Import (Backup og restore)

```bash
# Eksporter all data (offentlig + privat) til fil
./app export > backup.json

# Importer data
./app import < backup.json

# Importer og slett all eksisterende data først
./app import --clear < backup.json
```

## Konfigurering

Alle innstillinger går i `.env` fil:

```env
# HTTP Server
HTTP_PORT=8080
HTTP_LISTEN_ADDR=0.0.0.0
HTTP_WHITELIST=127.0.0.1/32,192.168.1.0/24

# WebSocket
WS_ENABLED=true

# Logging
LOG_ENABLED=false
LOG_PATH=system.log

# Cluster - Peer Sync
CLUSTER_PORT=9000
CLUSTER_LISTEN_ADDR=0.0.0.0
CLUSTER_PEERS=[2a03:333:111:d:2::aaab]:9000,[2a03:333:111:d:2::aaac]:9000
CLUSTER_CIPHER_KEY=base64-encoded-key
CLUSTER_PEER_SECRET=secret-key

# TCP Server (direkte protokoll)
TCP_PORT=9001
TCP_LISTEN_ADDR=0.0.0.0
TCP_KEY=your-tcp-key

# Autentisering
ADMIN_USERNAME=admin
ADMIN_PASSWORD=passord
```

## Data Struktur

### Lokalt (runtime)

```
runtime/
├── 2026-03-01_20-35-35/        (Timestamp folder)
│   ├── users.json              (Public array)
│   ├── private_secrets.json     (Private array)
│   └── ...
├── 2026-03-01_20-35-51/
│   └── ...
```

### JSON Format

```json
{
  "data": {
    "key1": "value1",
    "key2": "value2"
  },
  "timestamp": "2026-03-01T20:35:35.123456Z"
}
```

## Cluster Synkronisering

### Oppstart-synkronisering
```
1. Server starter
2. Laster data fra siste disk-snapshot (hvis --recover)
3. Kobler til alle peer-nodes
4. Spør: "GET_ARRAYS"
5. Mottar alle data + timestamps fra peers
6. Merger basert på timestamp: "Nyeste versjon vinner"
7. Lagrer til disk
```

### Periodisk bakgrunn-synk (hver 30 sek)
```
1. Spør alle peers: "GET_ARRAYS"
2. Sammenligner timestamps
3. Merger nyere data
4. Uppdaterer lokalt
```

## Metrics og Helse

Hver node rapporterer sin helse via `/cluster/metrics`:

```json
{
  "node": "0.0.0.0:8080",
  "bootup_time": "2026-03-01T20:35:35.123456Z",
  "uptime_seconds": 125.4,
  "health_status": "healthy",
  "configured_peers": 2,
  "responsive_peers": 2,
  "error_count": 0,
  "peer_bootup_times": {
    "node2:9000": "2026-03-01T20:34:00.000000Z",
    "node3:9000": "2026-03-01T20:33:45.000000Z"
  }
}
```

**Health Status:**
- `healthy` - Alle peers responderer
- `degraded` - Noen peers svarer ikke
- `disabled` - Ingen peers eller cluster disabled

## Eksempler

### Scenario 1: Multi-node cluster (3 noder)

```
Setup:
Node A: localhost:8080 (peer: B:9000, C:9000)
Node B: localhost:8081 (peer: A:9000, C:9000)
Node C: localhost:8082 (peer: A:9000, B:9000)

Scenario:
1. Skriv til Node A:
   curl -X POST localhost:8080/cluster/write?array=users -d '{"id1":"alice"}'

2. Node A lagrer lokalt + sender til B og C

3. Les fra Node B:
   curl localhost:8081/cluster/read?array=users
   → Returnerer: {"id1":"alice"}

4. Node B er nede, Node C har data

5. Node B starter igjen:
   ./app --recover
   → Laster fra disk + synkroniserer fra A og C
   → Har alle data igjen
```

### Scenario 2: Private data med autentisering

```
Setup:
- Public array: "users" (alle kan lese)
- Private array: "tokens" (bare autentisert)

1. Uautentisert bruker:
   curl localhost:8080/cluster/all
   → {"users": {...}}

2. Autentisert bruker:
   curl -u admin:passord localhost:8080/cluster/private/all
   → {"users": {...}, "tokens": {...}}

3. Skriv privat data:
   curl -u admin:passord -X POST localhost:8080/cluster/private/write?array=tokens \
     -d '{"token1":"secret"}'
```

## Sikkerhet

- **Peer-kommunikasjon**: AES-256-GCM kryptering
- **Admin-tilgang**: Basic Auth (username/password i .env)
- **TCP**: Nøkkel-basert autentisering
- **IP Whitelisting**: Støtte for CIDR ranges
- **Private Arrays**: Adskilt lagring, krever autentisering

## Troubleshooting

### Peer synkronisering feiler
```
Check:
1. Er alle peers tilgjengelige? curl node2:9000
2. Er CLUSTER_PEER_SECRET lik på alle noder?
3. Er CLUSTER_CIPHER_KEY lik på alle noder?
4. Sjekk logs: LOG_ENABLED=true, LOG_PATH=cluster.log
```

### Data blir ikke synkronisert
```
Check:
1. Sjekk metrics: curl localhost:8080/cluster/metrics
2. Er health_status "healthy"? (alle peers responderer)
3. Sjekk responsive_peers count
4. Trigger manual recovery: ./app --recover
```

### Private data ikke tilgjengelig
```
Check:
1. Bruker autentisering? curl -u username:password
2. Er ADMIN_USERNAME og ADMIN_PASSWORD korrekt i .env?
3. Sjekk at privat array eksisterer: /cluster/private/all
```

## Performance

- **Skriving**: Lokalt + asynkron sending til peers
- **Lesing**: Lokalt, ingen nettverkskall
- **Synkronisering**: Hver 30 sekunder (bakgrunn)
- **Oppstart**: ~5 sekunder (med retry)
- **Error Handling**: Exponential backoff (2, 4, 6, 8, 10 sekunder)

## Lisensiering

Denne prosjektet er åpen kildekode.

---

**Sist oppdatert**: 2. mars 2026
