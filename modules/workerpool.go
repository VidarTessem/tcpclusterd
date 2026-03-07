package modules

import (
	"fmt"
	"sync"
	"time"
)

// WorkerPool manages a pool of worker goroutines for processing requests
type WorkerPool struct {
	workers   int
	jobQueue  chan Job
	results   chan JobResult
	wg        sync.WaitGroup
	mu        sync.RWMutex
	isRunning bool
	logger    *Logger
	stats     WorkerStats
}

// Job represents a unit of work to be processed
type Job struct {
	ID       string
	Type     string // "query", "insert", "delete", "update"
	Handler  func() (interface{}, error)
	Priority int // Higher priority = processed first (future enhancement)
}

// JobResult represents the result of a processed job
type JobResult struct {
	JobID    string
	Result   interface{}
	Error    error
	Duration time.Duration
}

// WorkerStats tracks worker pool statistics
type WorkerStats struct {
	mu             sync.RWMutex
	JobsProcessed  int64
	JobsFailed     int64
	TotalDuration  time.Duration
	ActiveWorkers  int
	QueueLength    int
	MaxQueueLength int
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int, queueSize int, logger *Logger) *WorkerPool {
	if workers <= 0 {
		workers = 10 // Default to 10 workers
	}
	if queueSize <= 0 {
		queueSize = 1000 // Default queue size
	}
	if logger == nil {
		logger = GlobalLogger
	}

	wp := &WorkerPool{
		workers:   workers,
		jobQueue:  make(chan Job, queueSize),
		results:   make(chan JobResult, queueSize),
		logger:    logger,
		isRunning: true,
		stats: WorkerStats{
			MaxQueueLength: queueSize,
		},
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	// Start queue monitor
	go wp.monitorQueue()

	logger.Info("Worker pool started with %d workers (queue size: %d)", workers, queueSize)

	return wp
}

// worker processes jobs from the queue
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for job := range wp.jobQueue {
		if !wp.isRunning {
			return
		}

		wp.updateActiveWorkers(1)
		startTime := time.Now()

		result, err := job.Handler()
		duration := time.Since(startTime)

		wp.updateActiveWorkers(-1)

		// Update stats
		wp.stats.mu.Lock()
		if err != nil {
			wp.stats.JobsFailed++
		} else {
			wp.stats.JobsProcessed++
		}
		wp.stats.TotalDuration += duration
		wp.stats.mu.Unlock()

		// Send result if there's a listener
		select {
		case wp.results <- JobResult{
			JobID:    job.ID,
			Result:   result,
			Error:    err,
			Duration: duration,
		}:
		default:
			// Result channel full, skip
		}
	}
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job Job) error {
	wp.mu.RLock()
	if !wp.isRunning {
		wp.mu.RUnlock()
		return fmt.Errorf("worker pool is not running")
	}
	wp.mu.RUnlock()

	select {
	case wp.jobQueue <- job:
		return nil
	default:
		return fmt.Errorf("job queue is full")
	}
}

// SubmitAndWait submits a job and waits for the result
func (wp *WorkerPool) SubmitAndWait(job Job) (interface{}, error) {
	if err := wp.Submit(job); err != nil {
		return nil, err
	}

	// Wait for result
	for result := range wp.results {
		if result.JobID == job.ID {
			return result.Result, result.Error
		}
	}

	return nil, fmt.Errorf("no result received")
}

// Shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) Shutdown() {
	wp.mu.Lock()
	if !wp.isRunning {
		wp.mu.Unlock()
		return
	}
	wp.isRunning = false
	wp.mu.Unlock()

	wp.logger.Info("Shutting down worker pool...")

	close(wp.jobQueue)
	wp.wg.Wait()
	close(wp.results)

	wp.logger.Info("Worker pool shut down complete")
}

// GetStats returns current worker pool statistics
func (wp *WorkerPool) GetStats() WorkerStats {
	wp.stats.mu.RLock()
	defer wp.stats.mu.RUnlock()

	stats := wp.stats
	stats.QueueLength = len(wp.jobQueue)
	return stats
}

// updateActiveWorkers updates the active worker count
func (wp *WorkerPool) updateActiveWorkers(delta int) {
	wp.stats.mu.Lock()
	wp.stats.ActiveWorkers += delta
	if wp.stats.ActiveWorkers < 0 {
		wp.stats.ActiveWorkers = 0
	}
	wp.stats.mu.Unlock()
}

// monitorQueue monitors queue length and logs warnings
func (wp *WorkerPool) monitorQueue() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		wp.mu.RLock()
		if !wp.isRunning {
			wp.mu.RUnlock()
			return
		}
		wp.mu.RUnlock()

		queueLen := len(wp.jobQueue)
		wp.stats.mu.Lock()
		wp.stats.QueueLength = queueLen
		wp.stats.mu.Unlock()

		// Warn if queue is getting full
		if queueLen > wp.stats.MaxQueueLength*3/4 {
			wp.logger.Warn("Worker pool queue is %d%% full (%d/%d jobs)",
				queueLen*100/wp.stats.MaxQueueLength, queueLen, wp.stats.MaxQueueLength)
		}
	}
}
