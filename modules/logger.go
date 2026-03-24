package modules

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// LogLevel defines the verbosity level
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// Logger handles file and audit logging.
type Logger struct {
	mu            sync.Mutex
	logLevel      LogLevel
	consoleLogger *log.Logger
	fileLogger    *log.Logger
	logFile       *os.File
	journalPath   string
}

type journalEnvelope struct {
	Kind    string             `json:"kind"`
	DBWrite *WriteJournalEntry `json:"db_write,omitempty"`
	Cluster *JournalEntry      `json:"cluster,omitempty"`
}

var journalFileMu sync.Mutex

// NewLogger creates a new logger with console and optional file outputs
func NewLogger(logLevel string, logFilePath, auditFilePath string) *Logger {
	// Parse log level
	var level LogLevel
	switch logLevel {
	case "debug":
		level = DEBUG
	case "info":
		level = INFO
	case "warn":
		level = WARN
	case "error":
		level = ERROR
	default:
		level = INFO
	}

	logger := &Logger{
		logLevel:      level,
		consoleLogger: log.New(io.Discard, "[APP] ", log.LstdFlags),
		journalPath:   normalizeJournalPath(auditFilePath),
	}

	// Create logs directory if needed
	if logFilePath != "" || auditFilePath != "" {
		for _, path := range []string{logFilePath, logger.journalPath} {
			if path == "" {
				continue
			}
			logDir := filepath.Dir(path)
			if logDir != "" && logDir != "." {
				os.MkdirAll(logDir, 0755)
			}
		}
	}

	// Open log file
	if logFilePath != "" {
		file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Printf("Error opening log file: %v", err)
		} else {
			logger.logFile = file
			logger.fileLogger = log.New(file, "[APP] ", log.LstdFlags)
		}
	}

	return logger
}

func normalizeJournalPath(path string) string {
	trimmed := strings.TrimSpace(path)
	switch trimmed {
	case "", "logs/audit.log", "audit.log":
		return "journal.log"
	default:
		return trimmed
	}
}

func resolveJournalPath() string {
	if GlobalLogger != nil && strings.TrimSpace(GlobalLogger.journalPath) != "" {
		return GlobalLogger.journalPath
	}
	return normalizeJournalPath(os.Getenv("AUDIT_LOG_FILE"))
}

func loadJournalEnvelopesUnlocked() ([]journalEnvelope, error) {
	path := resolveJournalPath()
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []journalEnvelope{}, nil
		}
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	records := make([]journalEnvelope, 0)
	for {
		var record journalEnvelope
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func rewriteJournalEnvelopesUnlocked(records []journalEnvelope) error {
	path := resolveJournalPath()
	if path == "" {
		return nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func appendJournalEnvelope(record journalEnvelope) error {
	journalFileMu.Lock()
	defer journalFileMu.Unlock()
	path := resolveJournalPath()
	if path == "" {
		return nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(record)
}

func readJournalEnvelopes() ([]journalEnvelope, error) {
	journalFileMu.Lock()
	defer journalFileMu.Unlock()
	return loadJournalEnvelopesUnlocked()
}

func updateJournalEnvelopes(update func([]journalEnvelope) ([]journalEnvelope, error)) error {
	journalFileMu.Lock()
	defer journalFileMu.Unlock()
	records, err := loadJournalEnvelopesUnlocked()
	if err != nil {
		return err
	}
	updated, err := update(records)
	if err != nil {
		return err
	}
	return rewriteJournalEnvelopesUnlocked(updated)
}

// ResetReplicationJournal drops all persisted replication journal state.
// Use this after a verified full-state replacement, where any older per-write
// journal entries are stale and must not be replayed.
func ResetReplicationJournal() error {
	journalFileMu.Lock()
	defer journalFileMu.Unlock()
	return rewriteJournalEnvelopesUnlocked([]journalEnvelope{})
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.logLevel <= DEBUG {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.fileLogger != nil {
			l.fileLogger.Println("[DEBUG]", msg)
		} else {
			log.Println("[DEBUG]", msg)
		}
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	if l.logLevel <= INFO {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.fileLogger != nil {
			l.fileLogger.Println("[INFO]", msg)
		} else {
			log.Println("[INFO]", msg)
		}
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.logLevel <= WARN {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.fileLogger != nil {
			l.fileLogger.Println("[WARN]", msg)
		} else {
			log.Println("[WARN]", msg)
		}
	}
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fileLogger != nil {
		l.fileLogger.Println("[ERROR]", msg)
	} else {
		log.Println("[ERROR]", msg)
	}
}

// Audit logs an audit event (database writes, cluster operations, etc)
func (l *Logger) Audit(operation, details string) {
	timestamp := time.Now().Format(time.RFC3339)
	msg := fmt.Sprintf("OPERATION=%s TIMESTAMP=%s DETAILS=%s", operation, timestamp, details)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fileLogger != nil {
		l.fileLogger.Println("[AUDIT]", msg)
	} else {
		log.Println("[AUDIT]", msg)
	}
}

// Close closes all open log files
func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logFile != nil {
		l.logFile.Close()
	}
}

// Global logger instance
var GlobalLogger *Logger

// InitLogger initializes the global logger
func InitLogger(logLevel, logFile, auditLogFile string) {
	GlobalLogger = NewLogger(logLevel, logFile, auditLogFile)
}
