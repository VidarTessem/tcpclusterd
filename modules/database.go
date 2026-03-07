package modules

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Database represents the in-memory database with JSON file backing
type Database struct {
	mu                   sync.RWMutex
	runtimePath          string
	persistentBackupPath string
	data                 map[string]*DatabaseInstance // database_name -> DatabaseInstance
	users                map[string]*User             // username -> User
	adminUser            *User
	replicationPeers     []string
	autoClearRuntimes    bool
	autoBackupEnabled    bool
	writeJournal         []*WriteJournalEntry // Track all writes for replication
	journalMu            sync.RWMutex
}

// WriteJournalEntry represents a single write operation in the journal
type WriteJournalEntry struct {
	ID         string                 `json:"id"`
	Operation  string                 `json:"operation"` // insert, delete, add_user, configure_service
	Database   string                 `json:"database,omitempty"`
	Table      string                 `json:"table,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty"`
	Where      map[string]string      `json:"where,omitempty"`
	IsPrivate  bool                   `json:"is_private"`
	Timestamp  int64                  `json:"timestamp"`
	Replicated bool                   `json:"replicated"`
}

// DatabaseInstance represents a single database with public and private tables
type DatabaseInstance struct {
	Name          string                   `json:"name"`
	PublicTables  map[string][]interface{} `json:"public_tables"`
	PrivateTables map[string][]interface{} `json:"private_tables"`
	LastModified  time.Time                `json:"last_modified"`
}

// User represents a system user
type User struct {
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	IsAdmin      bool      `json:"is_admin"`
	CreatedAt    time.Time `json:"created_at"`
}

// NewDatabase creates a new database instance
func NewDatabase(runtimePath, persistentBackupPath string, autoClearRuntimes, autoBackupEnabled bool) *Database {
	return &Database{
		runtimePath:          runtimePath,
		persistentBackupPath: persistentBackupPath,
		data:                 make(map[string]*DatabaseInstance),
		users:                make(map[string]*User),
		autoClearRuntimes:    autoClearRuntimes,
		autoBackupEnabled:    autoBackupEnabled,
	}
}

// InitializeDatabase initializes the database with system database and admin user
func (db *Database) InitializeDatabase(adminPassword string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Create system database
	db.data["system"] = &DatabaseInstance{
		Name:          "system",
		PublicTables:  make(map[string][]interface{}),
		PrivateTables: make(map[string][]interface{}),
		LastModified:  time.Now(),
	}

	// Initialize users table (private)
	db.data["system"].PrivateTables["users"] = make([]interface{}, 0)

	// Initialize peer_metrics table (public) - tracks server health and stats
	db.data["system"].PublicTables["peer_metrics"] = make([]interface{}, 0)

	// Create admin user
	adminUser := &User{
		Username:     "admin",
		PasswordHash: db.hashPassword(adminPassword),
		IsAdmin:      true,
		CreatedAt:    time.Now(),
	}

	db.adminUser = adminUser
	db.users["admin"] = adminUser

	// Store admin user in system.users table
	userRow := map[string]interface{}{
		"username":      adminUser.Username,
		"password_hash": adminUser.PasswordHash,
		"is_admin":      true,
		"created_at":    adminUser.CreatedAt.Unix(),
	}

	db.data["system"].PrivateTables["users"] = append(
		db.data["system"].PrivateTables["users"],
		userRow,
	)

	return nil
}

// hashPassword hashes a password using SHA512
func (db *Database) hashPassword(password string) string {
	hash := sha512.Sum512([]byte(password))
	return hex.EncodeToString(hash[:])
}

// AuthenticateUser authenticates a user with username and password
func (db *Database) AuthenticateUser(username, password string) (*User, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	user, exists := db.users[username]
	if !exists {
		return nil, fmt.Errorf("user not found")
	}

	if db.hashPassword(password) != user.PasswordHash {
		return nil, fmt.Errorf("invalid password")
	}

	return user, nil
}

// AddUser adds a new user with a corresponding database
func (db *Database) AddUser(username, password string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if user already exists
	if _, exists := db.users[username]; exists {
		return fmt.Errorf("user already exists")
	}

	// Check if database already exists
	if _, exists := db.data[username]; exists {
		return fmt.Errorf("database already exists")
	}

	// Create user
	user := &User{
		Username:     username,
		PasswordHash: db.hashPassword(password),
		IsAdmin:      false,
		CreatedAt:    time.Now(),
	}

	db.users[username] = user

	// Create user's personal database
	db.data[username] = &DatabaseInstance{
		Name:          username,
		PublicTables:  make(map[string][]interface{}),
		PrivateTables: make(map[string][]interface{}),
		LastModified:  time.Now(),
	}

	// Store user in system.users table
	userRow := map[string]interface{}{
		"username":      user.Username,
		"password_hash": user.PasswordHash,
		"is_admin":      false,
		"created_at":    user.CreatedAt.Unix(),
	}

	db.data["system"].PrivateTables["users"] = append(
		db.data["system"].PrivateTables["users"],
		userRow,
	)

	return nil
}

// DeleteDatabase removes a database instance by name.
func (db *Database) DeleteDatabase(databaseName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if databaseName == "system" {
		return fmt.Errorf("cannot delete system database")
	}

	if _, exists := db.data[databaseName]; !exists {
		return nil
	}

	delete(db.data, databaseName)
	return nil
}

// userHasAccessToDatabase checks if a user has access to a database
func (db *Database) userHasAccessToDatabase(username, databaseName, accessType string) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()

	user, exists := db.users[username]
	if !exists {
		return false
	}

	// Admin users have access to all databases
	if user.IsAdmin {
		return true
	}

	// Non-admin users can only access their own database
	return username == databaseName
}

// InsertRow inserts a row into a table
func (db *Database) InsertRow(databaseName, tableName string, rowData map[string]interface{}, isPrivate bool) (interface{}, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	if isPrivate {
		if dbInstance.PrivateTables[tableName] == nil {
			dbInstance.PrivateTables[tableName] = make([]interface{}, 0)
		}
		dbInstance.PrivateTables[tableName] = append(dbInstance.PrivateTables[tableName], rowData)
	} else {
		if dbInstance.PublicTables[tableName] == nil {
			dbInstance.PublicTables[tableName] = make([]interface{}, 0)
		}
		dbInstance.PublicTables[tableName] = append(dbInstance.PublicTables[tableName], rowData)
	}

	dbInstance.LastModified = time.Now()

	// Log to write journal for replication
	db.LogWrite("insert", databaseName, tableName, rowData, nil, isPrivate)

	return map[string]interface{}{"status": "ok", "message": "row inserted"}, nil
}

// InsertRowDirect inserts a row without logging to journal (for replicated writes)
func (db *Database) InsertRowDirect(databaseName, tableName string, rowData map[string]interface{}, isPrivate bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return fmt.Errorf("database not found: %s", databaseName)
	}

	if isPrivate {
		if dbInstance.PrivateTables[tableName] == nil {
			dbInstance.PrivateTables[tableName] = make([]interface{}, 0)
		}
		dbInstance.PrivateTables[tableName] = append(dbInstance.PrivateTables[tableName], rowData)
	} else {
		if dbInstance.PublicTables[tableName] == nil {
			dbInstance.PublicTables[tableName] = make([]interface{}, 0)
		}
		dbInstance.PublicTables[tableName] = append(dbInstance.PublicTables[tableName], rowData)
	}

	dbInstance.LastModified = time.Now()
	return nil
}

// SelectRows selects rows from a table
func (db *Database) SelectRows(databaseName, tableName string, where map[string]string, isPrivate bool) (interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	var tableRef []interface{}
	if isPrivate {
		tableRef = dbInstance.PrivateTables[tableName]
	} else {
		tableRef = dbInstance.PublicTables[tableName]
	}

	if tableRef == nil {
		return []interface{}{}, nil
	}

	// Filter rows based on WHERE clause
	var results []interface{}
	for _, row := range tableRef {
		if len(where) == 0 {
			results = append(results, row)
		} else {
			// Simple filtering
			rowMap, ok := row.(map[string]interface{})
			if !ok {
				continue
			}

			matches := true
			for key, val := range where {
				if rowVal, ok := rowMap[key]; ok {
					if fmt.Sprintf("%v", rowVal) != val {
						matches = false
						break
					}
				} else {
					matches = false
					break
				}
			}

			if matches {
				results = append(results, row)
			}
		}
	}

	return results, nil
}

// GetAllTables returns all data from all tables in a database (public or private)
func (db *Database) GetAllTables(databaseName string, isPrivate bool) (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	result := make(map[string]interface{})

	if isPrivate {
		for tableName, rows := range dbInstance.PrivateTables {
			if rows != nil {
				result[tableName] = rows
			}
		}
	} else {
		for tableName, rows := range dbInstance.PublicTables {
			if rows != nil {
				result[tableName] = rows
			}
		}
	}

	return result, nil
}

// GetTablesCombined returns both public and private tables (for owner) or just public (for guests)
// isOwner = true if user is the owner of this database (username == databaseName)
// If a public and private table share the same name, rows are merged (public first, then private).
func (db *Database) GetTablesCombined(databaseName string, isOwner bool) (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	result := make(map[string]interface{})

	// Always include public tables
	for tableName, rows := range dbInstance.PublicTables {
		if rows != nil {
			cloned := make([]interface{}, 0, len(rows))
			cloned = append(cloned, rows...)
			result[tableName] = cloned
		}
	}

	// Include private tables only if user is the owner
	if isOwner {
		for tableName, rows := range dbInstance.PrivateTables {
			if rows != nil {
				if existing, ok := result[tableName]; ok {
					existingRows, ok := existing.([]interface{})
					if !ok {
						existingRows = []interface{}{}
					}
					merged := make([]interface{}, 0, len(existingRows)+len(rows))
					merged = append(merged, existingRows...)
					merged = append(merged, rows...)
					result[tableName] = merged
				} else {
					cloned := make([]interface{}, 0, len(rows))
					cloned = append(cloned, rows...)
					result[tableName] = cloned
				}
			}
		}
	}

	return result, nil
}

// DeleteRows deletes rows from a table
func (db *Database) DeleteRows(databaseName, tableName string, where map[string]string, isPrivate bool) (interface{}, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	var tableData []interface{}
	if isPrivate {
		tableData = dbInstance.PrivateTables[tableName]
	} else {
		tableData = dbInstance.PublicTables[tableName]
	}

	if tableData == nil {
		return map[string]interface{}{"status": "ok", "deleted": 0}, nil
	}

	// Filter rows to delete based on WHERE clause
	var newRows []interface{}
	deletedCount := 0

	for _, row := range tableData {
		if len(where) == 0 {
			deletedCount++
			continue
		}

		// Simple filtering
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			newRows = append(newRows, row)
			continue
		}

		matches := true
		for key, val := range where {
			if rowVal, ok := rowMap[key]; ok {
				if fmt.Sprintf("%v", rowVal) != val {
					matches = false
					break
				}
			} else {
				matches = false
				break
			}
		}

		if !matches {
			newRows = append(newRows, row)
		} else {
			deletedCount++
		}
	}

	if isPrivate {
		dbInstance.PrivateTables[tableName] = newRows
	} else {
		dbInstance.PublicTables[tableName] = newRows
	}

	dbInstance.LastModified = time.Now()

	// Log to write journal for replication
	db.LogWrite("delete", databaseName, tableName, nil, where, isPrivate)

	return map[string]interface{}{"status": "ok", "deleted": deletedCount}, nil
}

// DeleteRowsDirect deletes rows without logging to journal (for replicated writes)
func (db *Database) DeleteRowsDirect(databaseName, tableName string, where map[string]string, isPrivate bool) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return 0, fmt.Errorf("database not found: %s", databaseName)
	}

	var tableData []interface{}
	if isPrivate {
		tableData = dbInstance.PrivateTables[tableName]
	} else {
		tableData = dbInstance.PublicTables[tableName]
	}

	if tableData == nil {
		return 0, nil
	}

	// Filter rows to delete based on WHERE clause
	var newRows []interface{}
	deletedCount := 0

	for _, row := range tableData {
		if len(where) == 0 {
			deletedCount++
			continue
		}

		// Simple filtering
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			newRows = append(newRows, row)
			continue
		}

		matches := true
		for key, val := range where {
			if rowVal, ok := rowMap[key]; ok {
				if fmt.Sprintf("%v", rowVal) != val {
					matches = false
					break
				}
			} else {
				matches = false
				break
			}
		}

		if !matches {
			newRows = append(newRows, row)
		} else {
			deletedCount++
		}
	}

	if isPrivate {
		dbInstance.PrivateTables[tableName] = newRows
	} else {
		dbInstance.PublicTables[tableName] = newRows
	}

	dbInstance.LastModified = time.Now()
	return deletedCount, nil
}

// UpdateRows updates matching rows in a table
func (db *Database) UpdateRows(databaseName, tableName string, updateData map[string]interface{}, where map[string]string, isPrivate bool) (interface{}, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", databaseName)
	}

	var tableData []interface{}
	if isPrivate {
		tableData = dbInstance.PrivateTables[tableName]
	} else {
		tableData = dbInstance.PublicTables[tableName]
	}

	if tableData == nil {
		return map[string]interface{}{"status": "ok", "updated": 0}, nil
	}

	updatedCount := 0
	for i, row := range tableData {
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			continue
		}

		matches := true
		for key, val := range where {
			rowVal, exists := rowMap[key]
			if !exists || fmt.Sprintf("%v", rowVal) != val {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}

		for key, val := range updateData {
			rowMap[key] = val
		}
		tableData[i] = rowMap
		updatedCount++
	}

	if isPrivate {
		dbInstance.PrivateTables[tableName] = tableData
	} else {
		dbInstance.PublicTables[tableName] = tableData
	}

	dbInstance.LastModified = time.Now()
	db.LogWrite("update", databaseName, tableName, updateData, where, isPrivate)

	return map[string]interface{}{"status": "ok", "updated": updatedCount}, nil
}

// UpdateRowsDirect updates matching rows without journaling (for replicated writes)
func (db *Database) UpdateRowsDirect(databaseName, tableName string, updateData map[string]interface{}, where map[string]string, isPrivate bool) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	dbInstance, exists := db.data[databaseName]
	if !exists {
		return 0, fmt.Errorf("database not found: %s", databaseName)
	}

	var tableData []interface{}
	if isPrivate {
		tableData = dbInstance.PrivateTables[tableName]
	} else {
		tableData = dbInstance.PublicTables[tableName]
	}

	if tableData == nil {
		return 0, nil
	}

	updatedCount := 0
	for i, row := range tableData {
		rowMap, ok := row.(map[string]interface{})
		if !ok {
			continue
		}

		matches := true
		for key, val := range where {
			rowVal, exists := rowMap[key]
			if !exists || fmt.Sprintf("%v", rowVal) != val {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}

		for key, val := range updateData {
			rowMap[key] = val
		}
		tableData[i] = rowMap
		updatedCount++
	}

	if isPrivate {
		dbInstance.PrivateTables[tableName] = tableData
	} else {
		dbInstance.PublicTables[tableName] = tableData
	}

	dbInstance.LastModified = time.Now()
	return updatedCount, nil
}

func upsertWhereFromRow(rowData map[string]interface{}) map[string]string {
	if v, ok := rowData["id"]; ok && fmt.Sprintf("%v", v) != "" {
		return map[string]string{"id": fmt.Sprintf("%v", v)}
	}
	if v, ok := rowData["call_sid"]; ok && fmt.Sprintf("%v", v) != "" {
		return map[string]string{"call_sid": fmt.Sprintf("%v", v)}
	}
	return nil
}

// UpsertRow updates existing row (id/call_sid) or inserts a new row
func (db *Database) UpsertRow(databaseName, tableName string, rowData map[string]interface{}, isPrivate bool) (interface{}, error) {
	where := upsertWhereFromRow(rowData)
	if where == nil {
		return db.InsertRow(databaseName, tableName, rowData, isPrivate)
	}

	result, err := db.UpdateRows(databaseName, tableName, rowData, where, isPrivate)
	if err != nil {
		return nil, err
	}

	if resultMap, ok := result.(map[string]interface{}); ok {
		if updatedRaw, ok := resultMap["updated"]; ok && fmt.Sprintf("%v", updatedRaw) != "0" {
			return map[string]interface{}{"status": "ok", "operation": "updated", "updated": updatedRaw}, nil
		}
	}

	insertResult, err := db.InsertRow(databaseName, tableName, rowData, isPrivate)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"status": "ok", "operation": "inserted", "result": insertResult}, nil
}

// UpsertRowDirect updates or inserts without journaling (for replicated writes)
func (db *Database) UpsertRowDirect(databaseName, tableName string, rowData map[string]interface{}, isPrivate bool) error {
	where := upsertWhereFromRow(rowData)
	if where == nil {
		return db.InsertRowDirect(databaseName, tableName, rowData, isPrivate)
	}

	updated, err := db.UpdateRowsDirect(databaseName, tableName, rowData, where, isPrivate)
	if err != nil {
		return err
	}
	if updated > 0 {
		return nil
	}

	return db.InsertRowDirect(databaseName, tableName, rowData, isPrivate)
}

// ConfigureServiceDirect updates service configuration without logging to journal
// This is used during replication to avoid recursive journal entries
func (db *Database) ConfigureServiceDirect(serviceData map[string]interface{}) error {
	service, _ := serviceData["service"].(string)
	if service == "" {
		return fmt.Errorf("service name required")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	systemDB, exists := db.data["system"]
	if !exists {
		return fmt.Errorf("system database not found")
	}

	// Delete existing config for this service (direct manipulation)
	if systemDB.PrivateTables["services_config"] != nil {
		var newRows []interface{}
		for _, row := range systemDB.PrivateTables["services_config"] {
			rowMap, ok := row.(map[string]interface{})
			if !ok {
				newRows = append(newRows, row)
				continue
			}
			// Keep rows that don't match this service
			if rowService, ok := rowMap["service"].(string); !ok || rowService != service {
				newRows = append(newRows, row)
			}
		}
		systemDB.PrivateTables["services_config"] = newRows
	}

	// Insert new config (direct manipulation)
	if systemDB.PrivateTables["services_config"] == nil {
		systemDB.PrivateTables["services_config"] = make([]interface{}, 0)
	}
	systemDB.PrivateTables["services_config"] = append(systemDB.PrivateTables["services_config"], serviceData)
	systemDB.LastModified = time.Now()

	return nil
}

// ExportToJSON exports the entire database to JSON
func (db *Database) ExportToJSON() ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	exportData := map[string]interface{}{
		"timestamp": time.Now(),
		"databases": db.data,
	}

	return json.MarshalIndent(exportData, "", "  ")
}

// ExportToJSONDatabase exports a specific database to JSON (both public and private tables)
func (db *Database) ExportToJSONDatabase(dbname string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check if database exists
	database, exists := db.data[dbname]
	if !exists {
		return nil, fmt.Errorf("database '%s' not found", dbname)
	}

	// Export only this database
	exportData := map[string]interface{}{
		"timestamp": time.Now(),
		"databases": map[string]*DatabaseInstance{
			dbname: database,
		},
	}

	return json.MarshalIndent(exportData, "", "  ")
}

// ImportFromJSON imports database from JSON
func (db *Database) ImportFromJSON(data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	var importData struct {
		Databases map[string]*DatabaseInstance `json:"databases"`
		Users     map[string]*User             `json:"users"` // Legacy field, ignored
	}

	if err := json.Unmarshal(data, &importData); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	db.data = importData.Databases

	// Rebuild users map from system.users table
	db.users = make(map[string]*User)
	if systemDB, exists := db.data["system"]; exists {
		if usersTable, exists := systemDB.PrivateTables["users"]; exists {
			for _, userRow := range usersTable {
				if userMap, ok := userRow.(map[string]interface{}); ok {
					user := &User{
						Username:     userMap["username"].(string),
						PasswordHash: userMap["password_hash"].(string),
						IsAdmin:      userMap["is_admin"].(bool),
					}
					// Parse created_at
					if createdAt, ok := userMap["created_at"].(float64); ok {
						user.CreatedAt = time.Unix(int64(createdAt), 0)
					} else if createdAtStr, ok := userMap["created_at"].(string); ok {
						if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
							user.CreatedAt = t
						}
					}

					db.users[user.Username] = user
					if user.IsAdmin {
						db.adminUser = user
					}
				}
			}
		}
	}

	return nil
}

// SaveRuntimeSnapshot saves a snapshot of the database to the runtime folder
func (db *Database) SaveRuntimeSnapshot() error {
	jsonData, err := db.ExportToJSON()
	if err != nil {
		return fmt.Errorf("failed to export data: %w", err)
	}

	if db.runtimePath == "" {
		return fmt.Errorf("runtime path not set")
	}

	// Create runtime directory if it doesn't exist
	if err := os.MkdirAll(db.runtimePath, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %w", err)
	}

	// Save to database.json
	dbFilePath := filepath.Join(db.runtimePath, "database.json")
	if err := os.WriteFile(dbFilePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write database file: %w", err)
	}

	// Also save to persistent backup if enabled
	if db.autoBackupEnabled && db.persistentBackupPath != "" {
		if err := os.MkdirAll(db.persistentBackupPath, 0755); err != nil {
			return fmt.Errorf("failed to create backup directory: %w", err)
		}

		timestamp := time.Now().Format("2006-01-02_15-04-05")
		backupPath := filepath.Join(db.persistentBackupPath, fmt.Sprintf("backup_%s.json", timestamp))
		if err := os.WriteFile(backupPath, jsonData, 0644); err != nil {
			return fmt.Errorf("failed to write backup file: %w", err)
		}
	}

	return nil
}

// LoadRuntimeSnapshot loads a snapshot from the runtime folder
func (db *Database) LoadRuntimeSnapshot() error {
	if db.runtimePath == "" {
		return nil // No runtime path set, start blank
	}

	dbFilePath := filepath.Join(db.runtimePath, "database.json")
	if _, err := os.Stat(dbFilePath); os.IsNotExist(err) {
		return nil // No runtime file exists, start blank
	}

	jsonData, err := os.ReadFile(dbFilePath)
	if err != nil {
		return fmt.Errorf("failed to read database file: %w", err)
	}

	return db.ImportFromJSON(jsonData)
}

// GetAllDatabases returns all database names
func (db *Database) GetAllDatabases() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var names []string
	for name := range db.data {
		names = append(names, name)
	}
	return names
}

// extractIPFromAddress extracts the IP address from a "host:port" or "[ipv6]:port" format
func extractIPFromAddress(address string) string {
	// Handle IPv6 with brackets: [2a03:94e0:205b:d:1::aaab]:5000
	if strings.HasPrefix(address, "[") {
		if idx := strings.Index(address, "]"); idx > 0 {
			return address[1:idx]
		}
	}
	// Handle IPv4 or hostname: 192.168.1.1:5000
	if idx := strings.LastIndex(address, ":"); idx > 0 {
		return address[:idx]
	}
	return address
}

// UpdatePeerMetrics updates or creates a peer metrics entry, removing duplicates by IP (ignoring port)
func (db *Database) UpdatePeerMetrics(peerAddress string, isOnline bool, lastPing time.Time, replicationLag int64, writesReplicated int64, replicationFailures int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.data["system"] == nil {
		return fmt.Errorf("system database not initialized")
	}

	metrics := db.data["system"].PublicTables["peer_metrics"]
	if metrics == nil {
		db.data["system"].PublicTables["peer_metrics"] = make([]interface{}, 0)
		metrics = db.data["system"].PublicTables["peer_metrics"]
	}

	// Extract IP without port for comparison
	peerIP := extractIPFromAddress(peerAddress)

	// Find and update existing metric entry, or remove duplicates by IP (ignoring port)
	foundIndex := -1
	var newMetrics []interface{}
	for i, m := range metrics {
		if entry, ok := m.(map[string]interface{}); ok {
			if addr, ok := entry["peer_address"].(string); ok {
				// Compare IPs only, not full address with port
				existingIP := extractIPFromAddress(addr)
				if existingIP == peerIP {
					if foundIndex == -1 {
						foundIndex = i
						// Aggregate stats from duplicate entries
						if wr, ok := entry["writes_replicated"].(int64); ok {
							writesReplicated += wr
						} else if wr, ok := entry["writes_replicated"].(float64); ok {
							writesReplicated += int64(wr)
						}
						if rf, ok := entry["replication_failures"].(int64); ok {
							replicationFailures += rf
						} else if rf, ok := entry["replication_failures"].(float64); ok {
							replicationFailures += int64(rf)
						}
					}
					continue // Skip all existing entries for this IP (remove duplicates)
				}
			}
		}
		newMetrics = append(newMetrics, m)
	}

	// Create new entry
	newEntry := map[string]interface{}{
		"peer_address":         peerAddress,
		"is_online":            isOnline,
		"last_ping":            lastPing.Unix(),
		"replication_lag_ms":   replicationLag,
		"writes_replicated":    writesReplicated,
		"replication_failures": replicationFailures,
		"created_at":           time.Now().Unix(),
		"updated_at":           time.Now().Unix(),
	}

	newMetrics = append(newMetrics, newEntry)
	db.data["system"].PublicTables["peer_metrics"] = newMetrics
	db.data["system"].LastModified = time.Now()
	return nil
}

// GetPeerMetrics retrieves all peer metrics, filtering duplicates by IP (ignoring port) and stale entries
func (db *Database) GetPeerMetrics() ([]map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.data["system"] == nil {
		return nil, fmt.Errorf("system database not initialized")
	}

	metrics := db.data["system"].PublicTables["peer_metrics"]
	result := make([]map[string]interface{}, 0)
	// Track seen peers by IP (not full address) to filter duplicates (keep most recent)
	seenPeers := make(map[string]map[string]interface{})
	now := time.Now().Unix()

	for _, m := range metrics {
		if entry, ok := m.(map[string]interface{}); ok {
			if addr, ok := entry["peer_address"].(string); ok {
				// Skip stale entries (no ping for more than 5 minutes)
				var lastPing int64
				if lp, ok := entry["last_ping"].(float64); ok {
					lastPing = int64(lp)
				} else if lp, ok := entry["last_ping"].(int64); ok {
					lastPing = lp
				}
				if now-lastPing > 300 {
					continue // Skip stale entry
				}

				// Extract IP without port for deduplication
				peerIP := extractIPFromAddress(addr)

				// If we've seen this peer IP, keep the one with the latest ping and aggregate stats
				if existing, seen := seenPeers[peerIP]; seen {
					var existingPing int64
					if ep, ok := existing["last_ping"].(float64); ok {
						existingPing = int64(ep)
					} else if ep, ok := existing["last_ping"].(int64); ok {
						existingPing = ep
					}
					// Keep entry with latest ping, but aggregate stats
					if lastPing > existingPing {
						// Aggregate writes and failures from old entry
						var aggWrites, aggFailures int64
						if wr, ok := existing["writes_replicated"].(int64); ok {
							aggWrites += wr
						} else if wr, ok := existing["writes_replicated"].(float64); ok {
							aggWrites += int64(wr)
						}
						if rf, ok := existing["replication_failures"].(int64); ok {
							aggFailures += rf
						} else if rf, ok := existing["replication_failures"].(float64); ok {
							aggFailures += int64(rf)
						}
						// Add current entry stats
						if wr, ok := entry["writes_replicated"].(int64); ok {
							aggWrites += wr
						} else if wr, ok := entry["writes_replicated"].(float64); ok {
							aggWrites += int64(wr)
						}
						if rf, ok := entry["replication_failures"].(int64); ok {
							aggFailures += rf
						} else if rf, ok := entry["replication_failures"].(float64); ok {
							aggFailures += int64(rf)
						}
						entry["writes_replicated"] = aggWrites
						entry["replication_failures"] = aggFailures
						seenPeers[peerIP] = entry
					} else {
						// Keep existing, add stats from new entry
						if wr, ok := entry["writes_replicated"].(int64); ok {
							if ewr, ok := existing["writes_replicated"].(int64); ok {
								existing["writes_replicated"] = ewr + wr
							}
						} else if wr, ok := entry["writes_replicated"].(float64); ok {
							if ewr, ok := existing["writes_replicated"].(float64); ok {
								existing["writes_replicated"] = ewr + wr
							}
						}
						if rf, ok := entry["replication_failures"].(int64); ok {
							if erf, ok := existing["replication_failures"].(int64); ok {
								existing["replication_failures"] = erf + rf
							}
						} else if rf, ok := entry["replication_failures"].(float64); ok {
							if erf, ok := existing["replication_failures"].(float64); ok {
								existing["replication_failures"] = erf + rf
							}
						}
					}
				} else {
					seenPeers[peerIP] = entry
				}
			}
		}
	}

	for _, entry := range seenPeers {
		result = append(result, entry)
	}

	return result, nil
}

// GetPeerMetric retrieves metrics for a specific peer
func (db *Database) GetPeerMetric(peerAddress string) (map[string]interface{}, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.data["system"] == nil {
		return nil, fmt.Errorf("system database not initialized")
	}

	metrics := db.data["system"].PublicTables["peer_metrics"]
	for _, m := range metrics {
		if entry, ok := m.(map[string]interface{}); ok {
			if addr, ok := entry["peer_address"].(string); ok && addr == peerAddress {
				return entry, nil
			}
		}
	}

	return nil, fmt.Errorf("peer not found: %s", peerAddress)
}

// LogWrite adds a write operation to the journal for replication
func (db *Database) LogWrite(operation, database, table string, data map[string]interface{}, where map[string]string, isPrivate bool) {
	db.journalMu.Lock()
	defer db.journalMu.Unlock()

	entry := &WriteJournalEntry{
		ID:         fmt.Sprintf("%s-%d", operation, time.Now().UnixNano()),
		Operation:  operation,
		Database:   database,
		Table:      table,
		Data:       data,
		Where:      where,
		IsPrivate:  isPrivate,
		Timestamp:  time.Now().Unix(),
		Replicated: false,
	}

	db.writeJournal = append(db.writeJournal, entry)
}

// GetUnreplicatedWrites returns all writes that haven't been replicated yet
func (db *Database) GetUnreplicatedWrites() []*WriteJournalEntry {
	db.journalMu.RLock()
	defer db.journalMu.RUnlock()

	var unreplicated []*WriteJournalEntry
	for _, entry := range db.writeJournal {
		if !entry.Replicated {
			unreplicated = append(unreplicated, entry)
		}
	}
	return unreplicated
}

// MarkWriteReplicated marks a write as replicated
func (db *Database) MarkWriteReplicated(writeID string) {
	db.journalMu.Lock()
	defer db.journalMu.Unlock()

	for _, entry := range db.writeJournal {
		if entry.ID == writeID {
			entry.Replicated = true
			break
		}
	}
}

// ClearOldJournalEntries removes old replicated entries (older than 1 hour)
func (db *Database) ClearOldJournalEntries() {
	db.journalMu.Lock()
	defer db.journalMu.Unlock()

	now := time.Now().Unix()
	threshold := int64(3600) // 1 hour

	var filtered []*WriteJournalEntry
	for _, entry := range db.writeJournal {
		if !(entry.Replicated && (now-entry.Timestamp) > threshold) {
			filtered = append(filtered, entry)
		}
	}

	db.writeJournal = filtered
}
