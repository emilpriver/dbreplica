package dbreplica

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Add a variable to override sql.Open for mocking
var sqlOpen = sql.Open

// ReplicaManager manages multiple database connections including a primary write connection
// and multiple read replica connections with round-robin load balancing
type ReplicaManager struct {
	WriteDB       *sql.DB
	ReadDBs       []*sql.DB
	currentReadDB uint32 // atomic counter for round-robin selection
	mu            sync.RWMutex
	healthCheck   *HealthChecker
}

// Config contains configuration for the ReplicaManager
type Config struct {
	// How frequently to ping databases to check health
	HealthCheckInterval time.Duration
	// Database connection timeout
	ConnectTimeout time.Duration
}

// DefaultConfig provides sensible defaults for the ReplicaManager
func DefaultConfig() Config {
	return Config{
		HealthCheckInterval: 30 * time.Second,
		ConnectTimeout:      5 * time.Second,
	}
}

// NewReplicaManager creates a new ReplicaManager with the given write and read connection strings
func NewReplicaManager(writeConnStr string, readConnStrs []string, driverName string, config Config) (*ReplicaManager, error) {
	if writeConnStr == "" {
		return nil, errors.New("write connection string cannot be empty")
	}
	if len(readConnStrs) == 0 {
		return nil, errors.New("at least one read connection string is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.ConnectTimeout)
	defer cancel()

	// Open write connection
	writeDB, err := sqlOpen(driverName, writeConnStr)
	if err != nil {
		return nil, err
	}

	if err := writeDB.PingContext(ctx); err != nil {
		writeDB.Close()
		return nil, err
	}

	// Open read connections
	readDBs := make([]*sql.DB, 0, len(readConnStrs))
	for _, connStr := range readConnStrs {
		readDB, err := sqlOpen(driverName, connStr)
		if err != nil {
			// Close all previously opened connections
			writeDB.Close()
			for _, db := range readDBs {
				db.Close()
			}
			return nil, err
		}

		if err := readDB.PingContext(ctx); err != nil {
			writeDB.Close()
			for _, db := range readDBs {
				db.Close()
			}
			readDB.Close()
			return nil, err
		}

		readDBs = append(readDBs, readDB)
	}

	rm := &ReplicaManager{
		WriteDB:       writeDB,
		ReadDBs:       readDBs,
		currentReadDB: 0,
	}

	// Start health checker
	rm.healthCheck = NewHealthChecker(rm, config.HealthCheckInterval)
	rm.healthCheck.Start()

	return rm, nil
}

// Close closes all database connections
func (rm *ReplicaManager) Close() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Stop health checker
	if rm.healthCheck != nil {
		rm.healthCheck.Stop()
	}

	var errs []error

	// Close write connection
	if rm.WriteDB != nil {
		if err := rm.WriteDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close read connections
	for _, db := range rm.ReadDBs {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.New("failed to close one or more database connections")
	}

	return nil
}

// GetWriteDB returns the write database connection
func (rm *ReplicaManager) GetWriteDB() *sql.DB {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.WriteDB
}

// GetReadDB returns a read database connection using round-robin selection
func (rm *ReplicaManager) GetReadDB() *sql.DB {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if len(rm.ReadDBs) == 0 {
		// Fallback to write DB if no read replicas are available
		return rm.WriteDB
	}

	// Get the next read DB using atomic round-robin
	idx := atomic.AddUint32(&rm.currentReadDB, 1) % uint32(len(rm.ReadDBs))
	return rm.ReadDBs[int(idx)]
}

// Exec executes a query without returning any rows
// It automatically uses the write database for modifications
func (rm *ReplicaManager) Exec(query string, args ...interface{}) (sql.Result, error) {
	db := rm.DetermineDBForQuery(query)
	return db.Exec(query, args...)
}

// ExecContext executes a query without returning any rows with a context
// It automatically uses the write database for modifications
func (rm *ReplicaManager) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	db := rm.DetermineDBForQuery(query)
	return db.ExecContext(ctx, query, args...)
}

// Query executes a query that returns rows
// It uses read replicas for SELECT statements and write database for others
func (rm *ReplicaManager) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db := rm.DetermineDBForQuery(query)
	return db.Query(query, args...)
}

// QueryContext executes a query that returns rows with a context
// It uses read replicas for SELECT statements and write database for others
func (rm *ReplicaManager) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	db := rm.DetermineDBForQuery(query)
	return db.QueryContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row
// It uses read replicas for SELECT statements and write database for others
func (rm *ReplicaManager) QueryRow(query string, args ...interface{}) *sql.Row {
	db := rm.DetermineDBForQuery(query)
	return db.QueryRow(query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row with a context
// It uses read replicas for SELECT statements and write database for others
func (rm *ReplicaManager) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	db := rm.DetermineDBForQuery(query)
	return db.QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction using the write database
func (rm *ReplicaManager) Begin() (*sql.Tx, error) {
	return rm.GetWriteDB().Begin()
}

// BeginTx starts a transaction with options using the write database
func (rm *ReplicaManager) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return rm.GetWriteDB().BeginTx(ctx, opts)
}

// Prepare creates a prepared statement for later queries or executions
// It prepares on both write and read connections for flexibility
func (rm *ReplicaManager) Prepare(query string) (*PreparedStatement, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Prepare on write database
	writeStmt, err := rm.WriteDB.Prepare(query)
	if err != nil {
		return nil, err
	}

	// Prepare on all read databases
	readStmts := make([]*sql.Stmt, len(rm.ReadDBs))
	for i, db := range rm.ReadDBs {
		stmt, err := db.Prepare(query)
		if err != nil {
			// Close all previously prepared statements
			writeStmt.Close()
			for j := 0; j < i; j++ {
				readStmts[j].Close()
			}
			return nil, err
		}
		readStmts[i] = stmt
	}

	return &PreparedStatement{
		query:     query,
		writeStmt: writeStmt,
		readStmts: readStmts,
		rm:        rm,
	}, nil
}

// PrepareContext creates a prepared statement for later queries or executions with a context
func (rm *ReplicaManager) PrepareContext(ctx context.Context, query string) (*PreparedStatement, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Prepare on write database
	writeStmt, err := rm.WriteDB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	// Prepare on all read databases
	readStmts := make([]*sql.Stmt, len(rm.ReadDBs))
	for i, db := range rm.ReadDBs {
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			// Close all previously prepared statements
			writeStmt.Close()
			for j := 0; j < i; j++ {
				readStmts[j].Close()
			}
			return nil, err
		}
		readStmts[i] = stmt
	}

	return &PreparedStatement{
		query:     query,
		writeStmt: writeStmt,
		readStmts: readStmts,
		rm:        rm,
	}, nil
}

// DetermineDBForQuery decides whether to use read or write database
// based on the type of SQL statement
func (rm *ReplicaManager) DetermineDBForQuery(query string) *sql.DB {
	// Trim whitespace and get the first word to determine query type
	trimmedQuery := strings.TrimSpace(query)
	firstWord := strings.ToUpper(strings.Fields(trimmedQuery)[0])

	// Use write database for modifications
	switch firstWord {
	case "INSERT", "UPDATE", "DELETE", "UPSERT", "MERGE", "TRUNCATE", "ALTER", "CREATE", "DROP", "GRANT", "REVOKE", "LOCK", "UNLOCK":
		return rm.GetWriteDB()
	case "SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH":
		// For queries that start with WITH, check if they're actually CTE updates
		if firstWord == "WITH" && (strings.Contains(strings.ToUpper(trimmedQuery), " UPDATE ") ||
			strings.Contains(strings.ToUpper(trimmedQuery), " DELETE ") ||
			strings.Contains(strings.ToUpper(trimmedQuery), " INSERT ")) {
			return rm.GetWriteDB()
		}
		return rm.GetReadDB()
	default:
		// For any other query, use write database to be safe
		return rm.GetWriteDB()
	}
}

// PreparedStatement represents a prepared statement across multiple databases
type PreparedStatement struct {
	query     string
	writeStmt *sql.Stmt
	readStmts []*sql.Stmt
	rm        *ReplicaManager
	mu        sync.RWMutex
}

// Close closes the prepared statement
func (stmt *PreparedStatement) Close() error {
	stmt.mu.Lock()
	defer stmt.mu.Unlock()

	var errs []error

	// Close write statement
	if err := stmt.writeStmt.Close(); err != nil {
		errs = append(errs, err)
	}

	// Close read statements
	for _, s := range stmt.readStmts {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.New("failed to close one or more prepared statements")
	}

	return nil
}

// Exec executes a prepared statement
func (stmt *PreparedStatement) Exec(args ...interface{}) (sql.Result, error) {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.Exec(args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].Exec(args...)
}

// ExecContext executes a prepared statement with a context
func (stmt *PreparedStatement) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.ExecContext(ctx, args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].ExecContext(ctx, args...)
}

// Query executes a prepared query that returns rows
func (stmt *PreparedStatement) Query(args ...interface{}) (*sql.Rows, error) {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.Query(args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].Query(args...)
}

// QueryContext executes a prepared query that returns rows with a context
func (stmt *PreparedStatement) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.QueryContext(ctx, args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].QueryContext(ctx, args...)
}

// QueryRow executes a prepared query that is expected to return at most one row
func (stmt *PreparedStatement) QueryRow(args ...interface{}) *sql.Row {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.QueryRow(args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].QueryRow(args...)
}

// QueryRowContext executes a prepared query that is expected to return at most one row with a context
func (stmt *PreparedStatement) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	stmt.mu.RLock()
	defer stmt.mu.RUnlock()

	// Use write statement for modifications
	if stmt.rm.DetermineDBForQuery(stmt.query) == stmt.rm.GetWriteDB() {
		return stmt.writeStmt.QueryRowContext(ctx, args...)
	}

	// Use round-robin for read statements
	idx := atomic.AddUint32(&stmt.rm.currentReadDB, 1) % uint32(len(stmt.readStmts))
	return stmt.readStmts[int(idx)].QueryRowContext(ctx, args...)
}
