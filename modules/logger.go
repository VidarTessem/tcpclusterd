package modules

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

var (
	logFile     *os.File
	loggerMutex sync.Mutex
	multiWriter io.Writer
	logEnabled  bool
)

// InitLogger initializes the logging system based on environment configuration
func InitLogger(env map[string]string) error {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	// Check if logging is enabled
	if enabled, ok := env["LOG_ENABLED"]; ok && (enabled == "true" || enabled == "1") {
		logEnabled = true
		logPath := env["LOG_PATH"]
		if logPath == "" {
			logPath = "system.log"
		}

		// Open log file
		file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %v", err)
		}

		logFile = file
		multiWriter = io.MultiWriter(os.Stdout, file)
		log.SetOutput(multiWriter)
		log.Printf("[LOGGER] File logging enabled: %s", logPath)
	} else {
		logEnabled = false
		log.SetOutput(os.Stdout)
	}

	return nil
}

// CloseLogger closes the log file if it's open
func CloseLogger() {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	if logFile != nil {
		logFile.Close()
		logFile = nil
	}
}

// LogError logs an error and increments the error counter
func LogError(format string, v ...interface{}) {
	log.Printf("[ERROR] "+format, v...)
	IncrementErrorCount()
}

// IncrementErrorCount increments the cluster error counter
func IncrementErrorCount() {
	if clusterArray != nil {
		clusterArray.mu.Lock()
		clusterArray.errorCount++
		clusterArray.mu.Unlock()
	}
}
