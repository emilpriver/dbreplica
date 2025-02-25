package dbreplica

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// HealthChecker periodically checks the health of database connections
type HealthChecker struct {
	rm        *ReplicaManager
	interval  time.Duration
	stopChan  chan struct{}
	wg        sync.WaitGroup
	isRunning bool
	mu        sync.Mutex
}

// NewHealthChecker creates a new health checker for the replica manager
func NewHealthChecker(rm *ReplicaManager, interval time.Duration) *HealthChecker {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &HealthChecker{
		rm:       rm,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Start begins periodic health checks
func (hc *HealthChecker) Start() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.isRunning {
		return
	}

	hc.isRunning = true
	hc.stopChan = make(chan struct{})
	hc.wg.Add(1)

	go hc.run()
}

// Stop halts periodic health checks
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if !hc.isRunning {
		return
	}

	close(hc.stopChan)
	hc.wg.Wait()
	hc.isRunning = false
}

// run executes periodic health checks
func (hc *HealthChecker) run() {
	defer hc.wg.Done()

	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	// Run an initial health check
	hc.checkHealth()

	for {
		select {
		case <-ticker.C:
			hc.checkHealth()
		case <-hc.stopChan:
			return
		}
	}
}

// checkHealth pings all databases to check their health
func (hc *HealthChecker) checkHealth() {
	// Use a reasonable timeout for health checks
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check write DB first
	hc.checkDB(ctx, hc.rm.GetWriteDB(), true)

	// Check read DBs in parallel
	var wg sync.WaitGroup
	hc.rm.mu.RLock()
	for i, db := range hc.rm.ReadDBs {
		wg.Add(1)
		go func(db *sql.DB, index int) {
			defer wg.Done()
			hc.checkDB(ctx, db, false)
		}(db, i)
	}
	hc.rm.mu.RUnlock()

	wg.Wait()
}

// checkDB pings a database and handles errors
func (hc *HealthChecker) checkDB(ctx context.Context, db *sql.DB, isWrite bool) {
	err := db.PingContext(ctx)
	if err != nil {
		// Log the error (in a real-world scenario, you'd want to use a proper logger)
		// fmt.Printf("Health check failed for %s database: %v\n",
		//    isWrite ? "write" : "read", err)

		// In a production environment, you might want to:
		// 1. Emit metrics
		// 2. Try to reconnect
		// 3. Remove unhealthy replicas from the pool
		// 4. Notify administrators
	}
}
