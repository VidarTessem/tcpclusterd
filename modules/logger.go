package modules

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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

// Logger handles console, file, and audit logging
type Logger struct {
	mu            sync.Mutex
	logLevel      LogLevel
	consoleLogger *log.Logger
	fileLogger    *log.Logger
	auditLogger   *log.Logger
	logFile       *os.File
	auditFile     *os.File
}

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
		consoleLogger: log.New(os.Stdout, "[APP] ", log.LstdFlags),
	}

	// Create logs directory if needed
	if logFilePath != "" || auditFilePath != "" {
		logDir := filepath.Dir(logFilePath)
		if logDir != "" && logDir != "." {
			os.MkdirAll(logDir, 0755)
		}
	}

	// Open log file
	if logFilePath != "" {
		file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			logger.consoleLogger.Printf("Error opening log file: %v", err)
		} else {
			logger.logFile = file
			logger.fileLogger = log.New(file, "[APP] ", log.LstdFlags)
		}
	}

	// Open audit file
	if auditFilePath != "" {
		file, err := os.OpenFile(auditFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			logger.consoleLogger.Printf("Error opening audit log file: %v", err)
		} else {
			logger.auditFile = file
			logger.auditLogger = log.New(file, "[AUDIT] ", log.LstdFlags)
		}
	}

	return logger
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	if l.logLevel <= DEBUG {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		l.consoleLogger.Println("[DEBUG]", msg)
		if l.fileLogger != nil {
			l.fileLogger.Println("[DEBUG]", msg)
		}
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	if l.logLevel <= INFO {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		l.consoleLogger.Println("[INFO]", msg)
		if l.fileLogger != nil {
			l.fileLogger.Println("[INFO]", msg)
		}
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, v ...interface{}) {
	if l.logLevel <= WARN {
		msg := fmt.Sprintf(format, v...)
		l.mu.Lock()
		defer l.mu.Unlock()
		l.consoleLogger.Println("[WARN]", msg)
		if l.fileLogger != nil {
			l.fileLogger.Println("[WARN]", msg)
		}
	}
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.consoleLogger.Println("[ERROR]", msg)
	if l.fileLogger != nil {
		l.fileLogger.Println("[ERROR]", msg)
	}
}

// Audit logs an audit event (database writes, cluster operations, etc)
func (l *Logger) Audit(operation, details string) {
	timestamp := time.Now().Format(time.RFC3339)
	msg := fmt.Sprintf("OPERATION=%s TIMESTAMP=%s DETAILS=%s", operation, timestamp, details)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.consoleLogger.Println("[AUDIT]", msg)
	if l.auditLogger != nil {
		l.auditLogger.Println(msg)
	}
}

// Close closes all open log files
func (l *Logger) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.logFile != nil {
		l.logFile.Close()
	}
	if l.auditFile != nil {
		l.auditFile.Close()
	}
}

// Global logger instance
var GlobalLogger *Logger

// InitLogger initializes the global logger
func InitLogger(logLevel, logFile, auditLogFile string) {
	GlobalLogger = NewLogger(logLevel, logFile, auditLogFile)
}
