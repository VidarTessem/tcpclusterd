package modules

import (
	"fmt"
	"regexp"
	"strings"
)

// SQLCommand represents a parsed SQL command
type SQLCommand struct {
	Type     string            // INSERT, SELECT, DELETE, UPDATE
	Scope    string            // PRIVATE, PUBLIC
	Database string            // database name
	Table    string            // table name
	Columns  []string          // column names
	Values   []interface{}     // values to insert
	Where    map[string]string // WHERE conditions
	Raw      string            // raw query string
}

// SQLParser handles parsing SQL-like commands
type SQLParser struct {
	db *Database
}

// NewSQLParser creates a new SQL parser
func NewSQLParser(db *Database) *SQLParser {
	return &SQLParser{db: db}
}

// Parse parses a SQL command string
func (sp *SQLParser) Parse(query string) (*SQLCommand, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Check for INSERT
	if strings.HasPrefix(strings.ToUpper(query), "INSERT") {
		return sp.parseInsert(query)
	}

	// Check for SELECT
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		return sp.parseSelect(query)
	}

	// Check for DELETE
	if strings.HasPrefix(strings.ToUpper(query), "DELETE") {
		return sp.parseDelete(query)
	}

	// Check for UPDATE
	if strings.HasPrefix(strings.ToUpper(query), "UPDATE") {
		return sp.parseUpdate(query)
	}

	return nil, fmt.Errorf("unsupported query type")
}

// parseInsert parses INSERT commands
// INSERT PRIVATE INTO database.table (col1, col2) VALUES (val1, val2)
// INSERT PUBLIC INTO database.table (col1, col2) VALUES (val1, val2)
func (sp *SQLParser) parseInsert(query string) (*SQLCommand, error) {
	cmd := &SQLCommand{Type: "INSERT"}

	upperQuery := strings.ToUpper(query)

	// Determine scope (PRIVATE/PUBLIC)
	if strings.Contains(upperQuery, "INSERT PRIVATE") {
		cmd.Scope = "PRIVATE"
		query = strings.Replace(query, "INSERT PRIVATE", "INSERT", 1)
		upperQuery = strings.ToUpper(query)
	} else if strings.Contains(upperQuery, "INSERT PUBLIC") {
		cmd.Scope = "PUBLIC"
		query = strings.Replace(query, "INSERT PUBLIC", "INSERT", 1)
		upperQuery = strings.ToUpper(query)
	} else {
		// Default to PRIVATE
		cmd.Scope = "PRIVATE"
	}

	// Extract table name and columns
	// Pattern: INSERT INTO database.table (col1, col2) VALUES
	re := regexp.MustCompile(`INSERT\s+INTO\s+(\w+)\.(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) == 0 {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	cmd.Database = matches[1]
	cmd.Table = matches[2]

	// Parse columns
	cols := strings.Split(matches[3], ",")
	for _, col := range cols {
		cmd.Columns = append(cmd.Columns, strings.TrimSpace(col))
	}

	// Parse values
	vals := strings.Split(matches[4], ",")
	for _, val := range vals {
		val = strings.TrimSpace(val)
		// Remove quotes if present
		if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
			val = val[1 : len(val)-1]
		} else if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
			val = val[1 : len(val)-1]
		}
		// Check if it's a function call (like password())
		if strings.HasPrefix(strings.ToLower(val), "password(") {
			// Extract the actual value from password()
			inner := strings.TrimPrefix(strings.ToLower(val), "password(")
			inner = strings.TrimSuffix(inner, ")")
			inner = strings.TrimSpace(inner)
			// Remove quotes from inner value
			if strings.HasPrefix(inner, "'") && strings.HasSuffix(inner, "'") {
				inner = inner[1 : len(inner)-1]
			}
			hashedVal := sp.db.hashPassword(inner)
			cmd.Values = append(cmd.Values, hashedVal)
		} else {
			cmd.Values = append(cmd.Values, val)
		}
	}

	return cmd, nil
}

// parseSelect parses SELECT commands
// SELECT * FROM database.table WHERE key=value
// SELECT col1, col2 FROM database.table WHERE key=value
func (sp *SQLParser) parseSelect(query string) (*SQLCommand, error) {
	cmd := &SQLCommand{Type: "SELECT", Scope: "PRIVATE"}

	// Determine scope
	upperQuery := strings.ToUpper(query)
	if strings.Contains(upperQuery, "SELECT * FROM") {
		parts := strings.Split(query, "FROM")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid SELECT syntax")
		}
		fromPart := strings.TrimSpace(parts[1])

		// Extract database.table and WHERE clause
		whereParts := strings.Split(fromPart, "WHERE")
		tableSpec := strings.TrimSpace(whereParts[0])
		tableParts := strings.Split(tableSpec, ".")

		if len(tableParts) != 2 {
			return nil, fmt.Errorf("expected database.table format")
		}

		cmd.Database = strings.TrimSpace(tableParts[0])
		cmd.Table = strings.TrimSpace(tableParts[1])

		// Parse WHERE clause if present
		if len(whereParts) > 1 {
			whereClause := strings.TrimSpace(whereParts[1])
			cmd.Where = sp.parseWhereClause(whereClause)
		}

		return cmd, nil
	}

	return nil, fmt.Errorf("unsupported SELECT syntax")
}

// parseDelete parses DELETE commands
// DELETE FROM database.table WHERE key=value
// DELETE PRIVATE FROM database.table WHERE key=value
// DELETE PUBLIC FROM database.table WHERE key=value
func (sp *SQLParser) parseDelete(query string) (*SQLCommand, error) {
	cmd := &SQLCommand{Type: "DELETE", Scope: "PRIVATE"}

	upperQuery := strings.ToUpper(query)

	// Determine scope (PRIVATE/PUBLIC)
	if strings.Contains(upperQuery, "DELETE PRIVATE FROM") {
		cmd.Scope = "PRIVATE"
		query = strings.Replace(query, "DELETE PRIVATE", "DELETE", 1)
		upperQuery = strings.ToUpper(query)
	} else if strings.Contains(upperQuery, "DELETE PUBLIC FROM") {
		cmd.Scope = "PUBLIC"
		query = strings.Replace(query, "DELETE PUBLIC", "DELETE", 1)
		upperQuery = strings.ToUpper(query)
	}

	if !strings.Contains(upperQuery, "DELETE FROM") {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}

	parts := strings.Split(query, "FROM")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}

	fromPart := strings.TrimSpace(parts[1])

	// Extract database.table and WHERE clause
	whereParts := strings.Split(fromPart, "WHERE")
	tableSpec := strings.TrimSpace(whereParts[0])
	tableParts := strings.Split(tableSpec, ".")

	if len(tableParts) != 2 {
		return nil, fmt.Errorf("expected database.table format")
	}

	cmd.Database = strings.TrimSpace(tableParts[0])
	cmd.Table = strings.TrimSpace(tableParts[1])

	// Parse WHERE clause if present
	if len(whereParts) > 1 {
		whereClause := strings.TrimSpace(whereParts[1])
		cmd.Where = sp.parseWhereClause(whereClause)
	}

	return cmd, nil
}

// parseUpdate parses UPDATE commands
// UPDATE database.table SET col1=val1, col2=val2 WHERE key=value
func (sp *SQLParser) parseUpdate(query string) (*SQLCommand, error) {
	cmd := &SQLCommand{Type: "UPDATE", Scope: "PRIVATE"}

	upperQuery := strings.ToUpper(query)
	if !strings.Contains(upperQuery, "UPDATE") {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	// Extract table
	parts := strings.Split(query, "SET")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	tablePart := strings.TrimSpace(strings.Replace(parts[0], "UPDATE", "", 1))
	tableParts := strings.Split(tablePart, ".")

	if len(tableParts) != 2 {
		return nil, fmt.Errorf("expected database.table format")
	}

	cmd.Database = strings.TrimSpace(tableParts[0])
	cmd.Table = strings.TrimSpace(tableParts[1])

	return cmd, nil
}

// parseWhereClause parses WHERE conditions
func (sp *SQLParser) parseWhereClause(whereClause string) map[string]string {
	conditions := make(map[string]string)

	// Simple parser for WHERE key=value
	parts := strings.Split(whereClause, "=")
	if len(parts) >= 2 {
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(strings.Join(parts[1:], "="))

		// Remove quotes
		if strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'") {
			val = val[1 : len(val)-1]
		} else if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") {
			val = val[1 : len(val)-1]
		}

		conditions[key] = val
	}

	return conditions
}

// Execute executes a parsed SQL command
func (sp *SQLParser) Execute(cmd *SQLCommand, username string) (interface{}, error) {
	switch cmd.Type {
	case "INSERT":
		return sp.executeInsert(cmd, username)
	case "SELECT":
		return sp.executeSelect(cmd, username)
	case "DELETE":
		return sp.executeDelete(cmd, username)
	case "UPDATE":
		return sp.executeUpdate(cmd, username)
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// executeInsert inserts data into a table
func (sp *SQLParser) executeInsert(cmd *SQLCommand, username string) (interface{}, error) {
	isPrivate := cmd.Scope == "PRIVATE"

	// Check permissions
	if isPrivate && !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	if !isPrivate && !sp.db.userHasAccessToDatabase(username, cmd.Database, "ro") && !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	// Prepare row data
	rowData := make(map[string]interface{})
	for i, col := range cmd.Columns {
		if i < len(cmd.Values) {
			rowData[col] = cmd.Values[i]
		}
	}

	// Insert into storage
	return sp.db.InsertRow(cmd.Database, cmd.Table, rowData, isPrivate)
}

// executeSelect selects data from a table
func (sp *SQLParser) executeSelect(cmd *SQLCommand, username string) (interface{}, error) {
	// Check permissions
	if !sp.db.userHasAccessToDatabase(username, cmd.Database, "ro") && !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	// Retrieve data
	data, err := sp.db.SelectRows(cmd.Database, cmd.Table, cmd.Where, cmd.Scope == "PRIVATE")
	if err != nil {
		return nil, err
	}

	return data, nil
}

// executeDelete deletes data from a table
func (sp *SQLParser) executeDelete(cmd *SQLCommand, username string) (interface{}, error) {
	isPrivate := cmd.Scope == "PRIVATE"

	// Check permissions
	if !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	// Delete rows
	return sp.db.DeleteRows(cmd.Database, cmd.Table, cmd.Where, isPrivate)
}

// executeUpdate updates data in a table
func (sp *SQLParser) executeUpdate(cmd *SQLCommand, username string) (interface{}, error) {
	// Check permissions
	if !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	return nil, fmt.Errorf("UPDATE not yet implemented")
}

// ConvertJSONToSQLInsert converts JSON row to SQL INSERT statement
func ConvertJSONToSQLInsert(database, table string, data map[string]interface{}, isPrivate bool) string {
	scope := "PUBLIC"
	if isPrivate {
		scope = "PRIVATE"
	}

	var columns []string
	var values []string

	for k, v := range data {
		columns = append(columns, k)
		if str, ok := v.(string); ok {
			values = append(values, fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''")))
		} else {
			values = append(values, fmt.Sprintf("'%v'", v))
		}
	}

	return fmt.Sprintf("INSERT %s INTO %s.%s (%s) VALUES (%s)",
		scope, database, table, strings.Join(columns, ", "), strings.Join(values, ", "))
}
