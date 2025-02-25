# DB Replica Manager

A Go package for managing database connections with a single write primary and multiple read replicas using round-robin load balancing.

## Features

- Support for one write primary and multiple read replicas
- Automatic round-robin load balancing for read queries
- Intelligent query routing based on query type (SELECT vs INSERT/UPDATE/DELETE)
- Periodic health checks with concurrent monitoring
- Support for transactions, prepared statements, and contexts
- Graceful handling of database connection failures

## Installation

```bash
go get github.com/yourusername/dbreplica
```

## Usage

### Basic Usage

```go
package main

import (
    "log"
    "github.com/yourusername/dbreplica"
    _ "github.com/lib/pq" // Import your database driver
)

func main() {
    // Define connection strings
    writeConnString := "host=primary.example.com dbname=mydb user=user password=pass"
    readConnStrings := []string{
        "host=replica1.example.com dbname=mydb user=user password=pass",
        "host=replica2.example.com dbname=mydb user=user password=pass",
    }

    // Create a new replica manager with default config
    rm, err := dbreplica.NewReplicaManager(writeConnString, readConnStrings, "postgres", dbreplica.DefaultConfig())
    if err != nil {
        log.Fatalf("Failed to create replica manager: %v", err)
    }
    defer rm.Close()

    // Use rm.Query for SELECT queries (automatically uses read replicas)
    rows, err := rm.Query("SELECT id, name FROM users")
    
    // Use rm.Exec for write operations (automatically uses write primary)
    result, err := rm.Exec("UPDATE users SET status = $1 WHERE id = $2", "active", 1)
}
```

### Query Routing

The package automatically routes queries to the appropriate database:

- SELECT, SHOW, DESCRIBE, EXPLAIN queries go to read replicas
- INSERT, UPDATE, DELETE, TRUNCATE, ALTER, etc. go to the write primary
- WITH clauses are analyzed to determine if they contain write operations
- All transactions are handled by the write primary

### Health Checking

The package periodically checks the health of all database connections:

```go
config := dbreplica.DefaultConfig()
config.HealthCheckInterval = 15 * time.Second  // Custom health check interval
```

### Prepared Statements

The package supports prepared statements across all replicas:

```go
stmt, err := rm.Prepare("SELECT name FROM users WHERE id = $1")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

var name string
err = stmt.QueryRow(1).Scan(&name)
```

### Transactions

Transactions are always handled by the write primary:

```go
tx, err := rm.Begin()
if err != nil {
    log.Fatal(err)
}

// Operations within transaction
_, err = tx.Exec("UPDATE users SET status = $1", "active")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

## API Reference

### ReplicaManager

- `NewReplicaManager(writeConnStr string, readConnStrs []string, driverName string, config Config) (*ReplicaManager, error)`
- `Close() error`
- `Exec(query string, args ...interface{}) (sql.Result, error)`
- `ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)`
- `Query(query string, args ...interface{}) (*sql.Rows, error)`
- `QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)`
- `QueryRow(query string, args ...interface{}) *sql.Row`
- `QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row`
- `Begin() (*sql.Tx, error)`
- `BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)`
- `Prepare(query string) (*PreparedStatement, error)`
- `PrepareContext(ctx context.Context, query string) (*PreparedStatement, error)`
- `GetWriteDB() *sql.DB`
- `GetReadDB() *sql.DB`

### PreparedStatement

- `Close() error`
- `Exec(args ...interface{}) (sql.Result, error)`
- `ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error)`
- `Query(args ...interface{}) (*sql.Rows, error)`
- `QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error)`
- `QueryRow(args ...interface{}) *sql.Row`
- `QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row`

## License

MIT
