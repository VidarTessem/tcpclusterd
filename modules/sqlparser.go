package modules

import (
	"fmt"
	"regexp"
	"strconv"
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

	// Check for UPSERT
	if strings.HasPrefix(strings.ToUpper(query), "UPSERT") {
		return sp.parseUpsert(query)
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
	// Note: Use greedy (.*) for VALUES to capture JSON with parentheses
	re := regexp.MustCompile(`INSERT\s+INTO\s+(\w+)\.(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*)\)`)
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

	// Parse values - need to handle escaped quotes and commas inside strings
	valuesStr := matches[4]
	cmd.Values = sp.parseValuesList(valuesStr)

	return cmd, nil
}

// parseUpsert parses UPSERT commands
// UPSERT PRIVATE INTO database.table (col1, col2) VALUES (val1, val2)
// UPSERT PUBLIC INTO database.table (col1, col2) VALUES (val1, val2)
// UPSERT INTO database.table (...) VALUES (...) defaults to PRIVATE
func (sp *SQLParser) parseUpsert(query string) (*SQLCommand, error) {
	cmd := &SQLCommand{Type: "UPSERT"}

	upperQuery := strings.ToUpper(query)

	// Determine scope (PRIVATE/PUBLIC)
	if strings.Contains(upperQuery, "UPSERT PRIVATE") {
		cmd.Scope = "PRIVATE"
		query = strings.Replace(query, "UPSERT PRIVATE", "UPSERT", 1)
		upperQuery = strings.ToUpper(query)
	} else if strings.Contains(upperQuery, "UPSERT PUBLIC") {
		cmd.Scope = "PUBLIC"
		query = strings.Replace(query, "UPSERT PUBLIC", "UPSERT", 1)
		upperQuery = strings.ToUpper(query)
	} else {
		// Default to PRIVATE
		cmd.Scope = "PRIVATE"
	}

	// Pattern: UPSERT [PRIVATE|PUBLIC] INTO database.table (col1, col2) VALUES
	// Note: Use greedy (.*) for VALUES to capture JSON with parentheses
	re := regexp.MustCompile(`UPSERT\s+(?:PRIVATE\s+|PUBLIC\s+)?INTO\s+(\w+)\.(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*)\)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) == 0 {
		return nil, fmt.Errorf("invalid UPSERT syntax")
	}

	cmd.Database = matches[1]
	cmd.Table = matches[2]

	// Parse columns
	cols := strings.Split(matches[3], ",")
	for _, col := range cols {
		cmd.Columns = append(cmd.Columns, strings.TrimSpace(col))
	}

	// Parse values
	valuesStr := matches[4]
	cmd.Values = sp.parseValuesList(valuesStr)

	return cmd, nil
}

// parseValuesList parses comma-separated values respecting quotes and escapes
func (sp *SQLParser) parseValuesList(valuesStr string) []interface{} {
	// DEBUG: Log VALUES parsing input
	if GlobalLogger != nil {
		GlobalLogger.Debug("[VALUES:parse] input_len=%d preview=%s", len(valuesStr), truncateForLog(valuesStr, 150))
	}

	var values []interface{}
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(valuesStr); i++ {
		ch := valuesStr[i]

		if inQuote {
			if ch == quoteChar {
				// SQL escaped quote inside quoted value: '' or ""
				if i+1 < len(valuesStr) && valuesStr[i+1] == quoteChar {
					current.WriteByte(ch)
					i++
					continue
				}
				// End quote
				inQuote = false
				quoteChar = 0
				continue
			}
			current.WriteByte(ch)
			continue
		}

		if ch == '\'' || ch == '"' {
			inQuote = true
			quoteChar = ch
			continue
		}

		if ch == ',' {
			val := strings.TrimSpace(current.String())
			// Always append value, even if empty (empty strings are valid SQL values)
			val = sp.unescapeSQLString(val)
			// Try to convert to numeric type
			if num, err := strconv.ParseInt(val, 10, 64); err == nil {
				values = append(values, int(num))
			} else if num, err := strconv.ParseFloat(val, 64); err == nil {
				values = append(values, num)
			} else {
				values = append(values, val)
			}
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	// Add last value
	if current.Len() > 0 || (len(valuesStr) > 0 && valuesStr[len(valuesStr)-1] == ',') {
		val := strings.TrimSpace(current.String())
		// Always append value, even if empty (empty strings are valid SQL values)
		val = sp.unescapeSQLString(val)
		// Try to convert to numeric type
		if num, err := strconv.ParseInt(val, 10, 64); err == nil {
			values = append(values, int(num))
		} else if num, err := strconv.ParseFloat(val, 64); err == nil {
			values = append(values, num)
		} else {
			values = append(values, val)
		}
		GlobalLogger.Debug("[VALUES:parsed] count=%d first=%s last=%s", len(values), truncateForLog(getValueSafe(values, 0), 50), truncateForLog(getValueSafe(values, len(values)-1), 50))
		if len(values) > 8 {
			GlobalLogger.Debug("[VALUES:samples] v[8]=%s v[9]=%s v[10]=%s", truncateForLog(getValueSafe(values, 8), 40), truncateForLog(getValueSafe(values, 9), 40), truncateForLog(getValueSafe(values, 10), 40))
		}
	}

	return values
}

// Helper functions for debug logging
func truncateForLog(s string, maxLen int) string {
	str := fmt.Sprintf("%v", s)
	if len(str) <= maxLen {
		return str
	}
	return str[:maxLen] + "..."
}

func getValueSafe(values []interface{}, idx int) string {
	if idx < 0 || idx >= len(values) {
		return "<none>"
	}
	return fmt.Sprintf("%v", values[idx])
}

// unescapeSQLString reverses SQL escaping
func (sp *SQLParser) unescapeSQLString(s string) string {
	// Unescape in reverse order of escaping
	s = strings.ReplaceAll(s, "\\t", "\t")
	s = strings.ReplaceAll(s, "\\r", "\r")
	s = strings.ReplaceAll(s, "\\n", "\n")
	s = strings.ReplaceAll(s, "\\\"", "\"")
	s = strings.ReplaceAll(s, "''", "'")
	s = strings.ReplaceAll(s, "\\\\", "\\")
	return s
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

	re := regexp.MustCompile(`(?i)^UPDATE(?:\s+(PRIVATE|PUBLIC))?\s+(\w+)\.(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(query))
	if len(matches) == 0 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	scope := strings.ToUpper(strings.TrimSpace(matches[1]))
	if scope == "PUBLIC" {
		cmd.Scope = "PUBLIC"
	} else {
		cmd.Scope = "PRIVATE"
	}

	cmd.Database = strings.TrimSpace(matches[2])
	cmd.Table = strings.TrimSpace(matches[3])

	setClause := strings.TrimSpace(matches[4])
	if setClause == "" {
		return nil, fmt.Errorf("UPDATE requires SET clause")
	}

	assignments := sp.splitCSVRespectQuotes(setClause)
	for _, assignment := range assignments {
		kv := sp.splitAssignmentRespectQuotes(assignment)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid SET assignment: %s", assignment)
		}
		cmd.Columns = append(cmd.Columns, strings.TrimSpace(kv[0]))
		cmd.Values = append(cmd.Values, sp.parseSQLLiteral(kv[1]))
	}

	if len(matches) > 5 && strings.TrimSpace(matches[5]) != "" {
		cmd.Where = sp.parseWhereClause(strings.TrimSpace(matches[5]))
	}

	return cmd, nil
}

func (sp *SQLParser) splitCSVRespectQuotes(input string) []string {
	parts := make([]string, 0)
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)
	escaped := false

	for i, ch := range input {
		if escaped {
			current.WriteRune(ch)
			escaped = false
			continue
		}

		if ch == '\\' {
			escaped = true
			current.WriteRune(ch)
			continue
		}

		if (ch == '\'' || ch == '"') && !inQuote {
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
			continue
		}

		if inQuote && ch == quoteChar {
			if i+1 < len(input) && rune(input[i+1]) == quoteChar {
				// Escaped quote in SQL style ''
				current.WriteRune(ch)
				continue
			}
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
			continue
		}

		if ch == ',' && !inQuote {
			part := strings.TrimSpace(current.String())
			if part != "" {
				parts = append(parts, part)
			}
			current.Reset()
			continue
		}

		current.WriteRune(ch)
	}

	if tail := strings.TrimSpace(current.String()); tail != "" {
		parts = append(parts, tail)
	}

	return parts
}

func (sp *SQLParser) splitAssignmentRespectQuotes(input string) []string {
	inQuote := false
	quoteChar := rune(0)
	escaped := false

	for i, ch := range input {
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if (ch == '\'' || ch == '"') && !inQuote {
			inQuote = true
			quoteChar = ch
			continue
		}
		if inQuote && ch == quoteChar {
			inQuote = false
			quoteChar = 0
			continue
		}
		if ch == '=' && !inQuote {
			return []string{strings.TrimSpace(input[:i]), strings.TrimSpace(input[i+1:])}
		}
	}

	return nil
}

func (sp *SQLParser) parseSQLLiteral(raw string) interface{} {
	val := strings.TrimSpace(raw)
	if (strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) || (strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) {
		val = val[1 : len(val)-1]
	}
	return sp.unescapeSQLString(val)
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
	case "UPSERT":
		return sp.executeUpsert(cmd, username)
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
	isPrivate := cmd.Scope == "PRIVATE"

	// Check permissions
	if !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	updateData := make(map[string]interface{})
	for i, col := range cmd.Columns {
		if i < len(cmd.Values) {
			updateData[col] = cmd.Values[i]
		}
	}

	return sp.db.UpdateRows(cmd.Database, cmd.Table, updateData, cmd.Where, isPrivate)

}

// executeUpsert inserts or updates data in a table
func (sp *SQLParser) executeUpsert(cmd *SQLCommand, username string) (interface{}, error) {
	isPrivate := cmd.Scope == "PRIVATE"

	// Check permissions
	if isPrivate && !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	if !isPrivate && !sp.db.userHasAccessToDatabase(username, cmd.Database, "ro") && !sp.db.userHasAccessToDatabase(username, cmd.Database, "rw") {
		return nil, fmt.Errorf("access denied")
	}

	rowData := make(map[string]interface{})
	for i, col := range cmd.Columns {
		if i < len(cmd.Values) {
			rowData[col] = cmd.Values[i]
		}
	}

	return sp.db.UpsertRow(cmd.Database, cmd.Table, rowData, isPrivate)
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
