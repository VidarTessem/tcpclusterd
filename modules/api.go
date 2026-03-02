package modules

// Package modules provides a distributed cluster array library.
//
// Usage:
//   env := map[string]string{
//       "CLUSTER_PEERS": "localhost:9001,localhost:9002",
//       "CLUSTER_PORT": "9000",
//       "CLUSTER_CIPHER_KEY": "...",
//       "CLUSTER_PEER_SECRET": "...",
//   }
//   InitClusterArray(env, false)
//
//   Write("myarray", "key1: value1")
//   value, _ := Read("myarray", "key1")
//   Update("myarray", "key1: newvalue")
//   Delete("myarray", "key1")
//   GetAll() // Returns all arrays

// Write adds or updates a key-value pair in an array
func Write(arrayName, value string) error {
	return clusterArray.Write(arrayName, value)
}

// Read retrieves a value from an array
func Read(arrayName, key string) (string, error) {
	return clusterArray.Read(arrayName, key)
}

// Update modifies an existing key-value pair
func Update(arrayName, value string) error {
	return clusterArray.Update(arrayName, value)
}

// Delete removes a key from an array
func Delete(arrayName, key string) error {
	return clusterArray.Delete(arrayName, key)
}

// DeleteArray removes an entire array
func DeleteArray(arrayName string) error {
	return clusterArray.DeleteArray(arrayName)
}

// GetAll returns all arrays for use
func GetAll() map[string]map[string]string {
	return clusterArray.GetAll()
}

// GetArray returns one array by name.
// If the array does not exist, it returns an empty map.
func GetArray(arrayName string) map[string]string {
	return clusterArray.GetArray(arrayName)
}

// GetClusterInstance returns the underlying Cluster instance (for advanced usage like IP whitelist access)
func GetClusterInstance() *Cluster {
	return clusterArray
}

// GetMetrics returns cluster synchronization metrics
func GetMetrics() map[string]interface{} {
	return clusterArray.GetMetrics()
}

// GetAllWithMetrics returns arrays and metrics together
func GetAllWithMetrics() map[string]interface{} {
	return clusterArray.GetAllWithMetrics()
}

// GetAllWithMetricsAndPrivate returns both public and private arrays with metrics
func GetAllWithMetricsAndPrivate() map[string]interface{} {
	return clusterArray.GetAllWithMetricsAndPrivate()
}

// WritePrivate adds or updates a key-value pair in a private array
func WritePrivate(arrayName, value string) error {
	return clusterArray.WritePrivate(arrayName, value)
}

// ReadPrivate retrieves a value from a private array
func ReadPrivate(arrayName, key string) (string, error) {
	return clusterArray.ReadPrivate(arrayName, key)
}

// DeletePrivate removes a key from a private array
func DeletePrivate(arrayName, key string) error {
	return clusterArray.DeletePrivate(arrayName, key)
}

// DeletePrivateArray removes an entire private array
func DeletePrivateArray(arrayName string) error {
	return clusterArray.DeletePrivateArray(arrayName)
}

// Export exports all arrays to stdout as JSON
func Export() error {
	return clusterArray.Export()
}

// ExportArray exports one array to stdout as JSON.
// If the array does not exist, it exports {}.
func ExportArray(arrayName string) error {
	return clusterArray.ExportArray(arrayName)
}

// Import imports arrays from stdin (JSON format)
func Import(clearFirst bool) error {
	return clusterArray.Import(clearFirst)
}
