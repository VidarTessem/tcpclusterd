#!/bin/bash

# IP Whitelist Testing Examples
# This script demonstrates how to test the IP whitelisting feature

echo "=== IP Whitelist Testing Guide ==="
echo ""
echo "The cluster now supports IP whitelisting for both HTTP and peer-to-peer ports."
echo ""

echo "1. Configure .env file:"
echo "   HTTP_WHITELIST_IPS=127.0.0.1,::1"
echo "   CLUSTER_WHITELIST_IPS=127.0.0.1,::1"
echo ""

echo "2. Build the cluster:"
echo "   go build -o app"
echo ""

echo "3. Start the server:"
echo "   ./app"
echo ""

echo "4. Test HTTP access from whitelisted IP:"
echo "   curl -s http://127.0.0.1:8888/cluster/all | jq ."
echo ""

echo "5. Test HTTP access from non-whitelisted IP (if testing from another machine):"
echo "   curl -s http://192.168.1.X:8888/cluster/all"
echo "   Expected: 403 Forbidden - Access denied: IP not whitelisted"
echo ""

echo "6. View logs to see rejections:"
echo "   [HTTP] Rejected request from 192.168.1.X:YYYY: IP not whitelisted"
echo "   [CLUSTER] Rejected connection from 192.168.1.X:YYYY: IP not whitelisted"
echo ""

echo "=== Configuration Examples ==="
echo ""

echo "Allow only localhost:"
echo "  HTTP_WHITELIST_IPS=127.0.0.1,::1"
echo "  CLUSTER_WHITELIST_IPS=127.0.0.1,::1"
echo ""

echo "Allow specific network:"
echo "  HTTP_WHITELIST_IPS=192.168.1.0-255,127.0.0.1,::1"
echo "  CLUSTER_WHITELIST_IPS=192.168.1.0-255,127.0.0.1,::1"
echo ""

echo "Allow all IPv4 and IPv6:"
echo "  HTTP_WHITELIST_IPS=0.0.0.0,:"
echo "  CLUSTER_WHITELIST_IPS=0.0.0.0,:"
echo ""

echo "Allow nothing (no IPs):"
echo "  HTTP_WHITELIST_IPS=SPECIFIC.IP.ONLY"
echo "  CLUSTER_WHITELIST_IPS=SPECIFIC.IP.ONLY"
echo ""

echo "=== Implemented Features ==="
echo ""
echo "✓ HTTP endpoint protection"
echo "✓ Peer-to-peer port protection"
echo "✓ IPv4 and IPv6 support"
echo "✓ Special address handling (0.0.0.0, ::)"
echo "✓ Exact IP matching"
echo "✓ Early rejection (before auth)"
echo "✓ Audit logging"
echo "✓ Backward compatible (empty = allow all)"
echo ""

echo "=== Security Order ==="
echo ""
echo "1. IP Whitelist Check (earliest)"
echo "2. Authentication (peer secret)"
echo "3. Data Decryption/Processing"
echo ""
echo "Unauthorized IPs are rejected immediately without processing."
