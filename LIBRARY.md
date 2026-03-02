# Cluster - A Distributed Array Library

Cluster is a reusable Go library that provides a distributed, encrypted, synchronized array storage system perfect for building distributed applications.

## Features

- **Distributed Synchronization**: Automatic peer-to-peer data synchronization with conflict resolution
- **AES-256 Encryption**: All inter-node communication is encrypted
- **Peer Authentication**: Shared secret authentication between cluster nodes
- **Persistent Storage**: Automatic JSON serialization to disk with timestamped runtime directories
- **Timestamp-based Conflict Resolution**: Latest data wins in conflicts
- **Background Workers**: Non-blocking operations with debounced broadcasting
- **Export/Import**: Full backup and restore capabilities
- **Configurable**: Environment-based configuration system

## Installation

Simply import the modules package into your Go project:

```go
import "cluster/modules"
```

## Quick Start

### 1. Initialize the Cluster

```go
package main

import (
	"cluster/modules"
)

func main() {
	env := map[string]string{
		"CLUSTER_PEERS": "localhost:9001,localhost:9002",
		"CLUSTER_PORT": "9000",
		"CLUSTER_LISTEN_ADDR": "[::1]",
		"HTTP_LISTEN_ADDR": "0.0.0.0",
		"CLUSTER_CIPHER_KEY": "your-base64-encoded-32-byte-key",
		"CLUSTER_PEER_SECRET": "your-shared-secret",
	}
	
	modules.InitClusterArray(env, false)
	
	// Server will now be running with HTTP on :8888 and peer sync on :9000
}
```

### 2. Use the API

```go
// Write
modules.Write("users", "user1: john@example.com")

// Read
value, err := modules.Read("users", "user1")
if err != nil {
	fmt.Println("Error:", err)
}

// Update
modules.Update("users", "user1: john.doe@example.com")

// Delete key
modules.Delete("users", "user1")

// Delete entire array
modules.DeleteArray("users")

// Get all arrays
all := modules.GetAll()
fmt.Println(all)

// Get metrics
metrics := modules.GetMetrics()
fmt.Println(metrics)

// Get arrays with metrics
allWithMetrics := modules.GetAllWithMetrics()
fmt.Println(allWithMetrics)
```

## HTTP API

The library also provides HTTP endpoints when initialized with `InitClusterArray`:

- `GET /` - Get all arrays with metrics
- `POST /cluster/write?array=<name>&key=<key>&value=<value>` - Write a value
- `GET /cluster/read?array=<name>&key=<key>` - Read a value
- `GET /cluster/all` - Get all arrays with metrics
- `DELETE /cluster/delete?array=<name>[&key=<key>]` - Delete a key or array

## Command-Line Interface

The included server binary supports several commands:

```bash
# Start server
./app

# Start server and load from last runtime state
./app --lastconfig

# Export all arrays to stdout
./app export > backup.json

# Import arrays from stdin
cat backup.json | ./app import

# Import and clear existing data first
cat backup.json | ./app import --clear
```

## Configuration

Configure via `.env` file:

```env
# HTTP server port
PORT=8888

# Cluster peer addresses (comma-separated)
CLUSTER_PEERS=localhost:9001,localhost:9002

# Port for peer-to-peer synchronization
CLUSTER_PORT=9000

# Listen address for peer synchronization
CLUSTER_LISTEN_ADDR=[::1]

# Listen address for HTTP server
HTTP_LISTEN_ADDR=0.0.0.0

# AES-256 encryption key (32 bytes, base64 encoded)
# Generate with: openssl rand -base64 32
CLUSTER_CIPHER_KEY=dGhpcyBpcyBhIDMyIGJ5dGUga2V5IGZvciBBRVMtMjU2IQ==

# Shared secret for peer authentication
CLUSTER_PEER_SECRET=your-shared-secret-here
```

## Architecture

### Public API (modules/api.go)

Clean, simple wrapper functions that delegate to the internal Cluster instance:

```go
Write(arrayName, value string) error
Read(arrayName, key string) (string, error)
Update(arrayName, value string) error
Delete(arrayName, key string) error
DeleteArray(arrayName string) error
GetAll() map[string]map[string]string
GetMetrics() map[string]interface{}
GetAllWithMetrics() map[string]interface{}
Export() error
Import(clearFirst bool) error
InitClusterArray(env map[string]string, loadLastConfig bool)
StartHTTPServer(loadLastConfig bool) error
```

### Core Implementation (modules/clusterarray.go)

The `Cluster` struct with receiver methods:

- `Write(arrayName, value string)` - Add/update entry
- `Read(arrayName, key string)` - Retrieve value
- `Update(arrayName, value string)` - Modify existing entry
- `Delete(arrayName, key string)` - Remove entry
- `DeleteArray(arrayName string)` - Remove array
- `GetAll()` - Get all arrays
- `GetMetrics()` - Get sync statistics
- `UpdateFromPeer()` - Merge peer updates
- `Export()` - Export to stdout
- `Import()` - Import from stdin
- `encryptMessage()` - AES-256 encryption
- `decryptMessage()` - AES-256 decryption
- `saveArrayToDisk()` - Persist to disk
- `deleteArrayFromDisk()` - Remove persisted array
- `handlePeerConnection()` - Accept peer connections
- `syncFromPeersOnStartup()` - Initial peer sync
- `periodicPeerSync()` - Background sync every 30s
- `broadcastToPeers()` - Signal peer broadcast
- `broadcastWorker()` - Process broadcasts with debouncing

### HTTP Server (modules/http.go)

Provides REST API endpoints using the public API.

### Synchronization Strategy

1. **Startup**: Connect to all peers and fetch their data (with retry/backoff)
2. **On Write/Update/Delete**: Signal broadcast (non-blocking)
3. **Broadcast Worker**: Debounce for 100ms, then send to all peers
4. **Periodic Sync**: Every 30 seconds, fetch latest data from peers
5. **Conflict Resolution**: Timestamp-based - newest data wins

## Use Cases

- **Distributed Configuration**: Share configuration across cluster nodes
- **Session Storage**: Replicate session data across servers
- **Cache**: Distributed cache with persistence
- **State Management**: Cluster-wide state synchronization
- **Key-Value Store**: Simple distributed KV store with encryption

## Security

- **AES-256-GCM**: Authenticated encryption for all peer communication
- **Peer Secret**: Shared secret authentication before accepting connections
- **Environment-based Configuration**: Sensitive data from environment variables

## Performance

- **Non-blocking I/O**: All file operations run asynchronously
- **Debounced Broadcasting**: Reduces network traffic by batching updates
- **Read-Write Locks**: Concurrent reads with exclusive writes
- **Buffered Channels**: 100-slot buffer for broadcast queue

## Error Handling

All public API methods return errors:

```go
if err := modules.Write("array", "key: value"); err != nil {
	log.Printf("Write failed: %v", err)
}
```

Common errors:
- "array not found" - Referenced array doesn't exist
- "key not found" - Referenced key doesn't exist
- "invalid format" - Value format incorrect (expected "key: value")
- Encryption/decryption errors - Key/auth issues

## Reusing in Other Projects

To use this library in another Go project:

1. Copy the `modules/` directory to your project
2. Import: `import "yourmodule/modules"`
3. Call the public API functions
4. Configure via environment variables or by building the env map manually

Example standalone usage:

```go
package main

import (
	"fmt"
	"log"
	"yourmodule/modules"
)

func main() {
	// Configure
	env := map[string]string{
		"CLUSTER_PEERS": "peer1:9000,peer2:9000",
		"CLUSTER_PORT": "9000",
		"CLUSTER_LISTEN_ADDR": "::",
		"CLUSTER_CIPHER_KEY": "your-key-here",
		"CLUSTER_PEER_SECRET": "your-secret-here",
	}
	
	// Initialize
	modules.InitClusterArray(env, false)
	
	// Use
	modules.Write("data", "key1: value1")
	val, _ := modules.Read("data", "key1")
	fmt.Println("Value:", val)
}
```

## Testing

Test the HTTP API:

```bash
# Terminal 1: Start server
./app

# Terminal 2: Write data
curl -X POST "http://localhost:8888/cluster/write?array=test&key=foo&value=bar"

# Read data
curl "http://localhost:8888/cluster/read?array=test&key=foo"

# Get all
curl http://localhost:8888/

# Delete key
curl -X DELETE "http://localhost:8888/cluster/delete?array=test&key=foo"
```

## License

MIT

## Contributing

This is a demonstration of converting a monolithic application into a reusable library. The key refactoring includes:

1. **Private vs Public**: Private fields (`data`, `timestamps`) with receiver methods
2. **Wrapper Functions**: Public API in `api.go` delegates to receiver methods
3. **Singleton Pattern**: Global `clusterArray` instance managed internally
4. **Clean Separation**: Users interact through simple public functions, not struct methods directly
5. **Flexibility**: Can be used as a library or standalone application

This pattern makes the code flexible for both standalone use and library integration.
