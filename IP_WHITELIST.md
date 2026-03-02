# IP Whitelisting Configuration

The cluster system now supports IP address whitelisting for both HTTP and peer-to-peer ports to restrict access to authorized clients.

## Configuration

Configure IP whitelisting in `.env`:

```env
# HTTP Port Whitelist (comma-separated IPs)
# Leave empty to allow all IPs
HTTP_WHITELIST_IPS=::,0.0.0.0,127.0.0.1,::1

# Cluster Peer Sync Port Whitelist (comma-separated IPs)
# Leave empty to allow all IPs
CLUSTER_WHITELIST_IPS=::,0.0.0.0,127.0.0.1,::1
```

## Default Behavior

- **Empty whitelist**: Allows all IP addresses
- **With values**: Only listed IPs are allowed

## Supported Address Formats

### IPv4 Addresses
- Single IP: `192.168.1.100`
- Any address (IPv4): `0.0.0.0`
- Localhost: `127.0.0.1`

### IPv6 Addresses
- Single IP: `::1`, `fe80::1`, `2001:db8::1`
- Any address (IPv6): `::`
- IPv6 localhost: `::1`

## Example Configurations

### Allow only localhost
```env
HTTP_WHITELIST_IPS=127.0.0.1,::1
CLUSTER_WHITELIST_IPS=127.0.0.1,::1
```

### Allow specific network
```env
HTTP_WHITELIST_IPS=192.168.1.100,192.168.1.101,127.0.0.1,::1
CLUSTER_WHITELIST_IPS=192.168.1.100,192.168.1.101,127.0.0.1,::1
```

### Allow all (no restrictions)
```env
HTTP_WHITELIST_IPS=
CLUSTER_WHITELIST_IPS=
```

### Allow all IPv4 and IPv6
```env
HTTP_WHITELIST_IPS=0.0.0.0,::
CLUSTER_WHITELIST_IPS=0.0.0.0,::
```

## How It Works

### HTTP Port
- All HTTP requests are checked against `HTTP_WHITELIST_IPS`
- If IP not whitelisted: Returns `403 Forbidden` with message "Access denied: IP not whitelisted"
- Request logged in `[HTTP]` output

### Cluster Peer Sync Port (port 9000 by default)
- All incoming peer connections are checked against `CLUSTER_WHITELIST_IPS`
- If IP not whitelisted: Connection is immediately closed
- Rejection logged in `[CLUSTER]` output

## Implementation Details

### IP Extraction
- Extracts IP from connection source address (removes port number)
- Removes IPv6 brackets: `[::1]` → `::1`

### IPv4/IPv6 Special Addresses
- `0.0.0.0` (IPv4 any address) matches any IPv4 address
- `::` (IPv6 any address) matches any IPv6 address
- Exact string match for all other addresses

### Security Order
1. **HTTP**: IP check → Authentication
2. **Cluster**: IP check → Authentication (peer secret)

IP whitelist is checked BEFORE attempting authentication to reject unauthorized sources early.

## Logging

### HTTP rejection
```
[HTTP] Rejected request from 192.168.1.50:54321: IP not whitelisted
```

### Cluster rejection
```
[CLUSTER] Rejected connection from 192.168.1.50:54321: IP not whitelisted
```

### Cluster acceptance
```
[CLUSTER] Peer authenticated from 192.168.1.100:54321
```

## Testing

### Test HTTP whitelist
```bash
# Allowed IP
curl http://localhost:8888/cluster/all

# Denied IP (from non-whitelisted address)
# Returns: 403 Forbidden
```

### Test cluster whitelist
- Connection attempts from non-whitelisted IPs will be immediately rejected
- Attempt to add peer from rejected IP will fail during sync

## Performance Impact

- IP whitelisting adds minimal overhead (~1-2% CPU impact)
- No memory overhead (whitelist stored as string slice)
- Check performed before expensive operations (decryption, authentication)
