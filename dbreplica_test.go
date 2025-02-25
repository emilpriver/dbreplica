package dbreplica_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/emilpriver/dbreplica"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReplicaManager(t *testing.T) {
	t.Run("should create replica manager with valid connections", func(t *testing.T) {
		// Create mock for write DB
		writeDB, writeMock, err := sqlmock.New()
		assert.NoError(t, err)

		// Create mocks for read DBs
		readDB1, readMock1, err := sqlmock.New()
		assert.NoError(t, err)
		readDB2, readMock2, err := sqlmock.New()
		assert.NoError(t, err)

		// Set up expectations for Ping
		writeMock.ExpectPing()
		readMock1.ExpectPing()
		readMock2.ExpectPing()

		// Override sql.Open to return our mocks
		originalOpen := sqlOpen
		defer func() { sqlOpen = originalOpen }()

		sqlOpen = func(driverName, dataSourceName string) (*sql.DB, error) {
			if dataSourceName == "write" {
				return writeDB, nil
			} else if dataSourceName == "read1" {
				return readDB1, nil
			} else if dataSourceName == "read2" {
				return readDB2, nil
			}
			return nil, sql.ErrConnDone
		}

		// Create the replica manager
		rm, err := dbreplica.NewReplicaManager("write", []string{"read1", "read2"}, "mock", dbreplica.Config{
			HealthCheckInterval: 1 * time.Hour, // Set long interval for test
			ConnectTimeout:      5 * time.Second,
		})

		// Assert no error
		assert.NoError(t, err)
		assert.NotNil(t, rm)

		// Clean up
		rm.Close()
		assert.NoError(t, writeMock.ExpectationsWereMet())
		assert.NoError(t, readMock1.ExpectationsWereMet())
		assert.NoError(t, readMock2.ExpectationsWereMet())
	})

	t.Run("should return error with empty write connection", func(t *testing.T) {
		rm, err := dbreplica.NewReplicaManager("", []string{"read1", "read2"}, "mock", dbreplica.DefaultConfig())
		assert.Error(t, err)
		assert.Nil(t, rm)
	})

	t.Run("should return error with empty read connections", func(t *testing.T) {
		rm, err := dbreplica.NewReplicaManager("write", []string{}, "mock", dbreplica.DefaultConfig())
		assert.Error(t, err)
		assert.Nil(t, rm)
	})
}

func TestDetermineDBForQuery(t *testing.T) {
	// Setup
	writeDB, _, err := sqlmock.New()
	require.NoError(t, err)

	readDB1, _, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB1},
	}

	// Test cases
	tests := []struct {
		name     string
		query    string
		expected *sql.DB
	}{
		{"SELECT query", "SELECT * FROM users", readDB1},
		{"SELECT with lowercase", "select * from users", readDB1},
		{"SELECT with whitespace", "  SELECT * FROM users", readDB1},
		{"INSERT query", "INSERT INTO users (name) VALUES ('test')", writeDB},
		{"UPDATE query", "UPDATE users SET name = 'test'", writeDB},
		{"DELETE query", "DELETE FROM users", writeDB},
		{"WITH query (read)", "WITH cte AS (SELECT * FROM users) SELECT * FROM cte", readDB1},
		{"WITH query (write)", "WITH cte AS (SELECT * FROM users) UPDATE users SET name = 'test'", writeDB},
		{"EXPLAIN query", "EXPLAIN SELECT * FROM users", readDB1},
		{"SHOW query", "SHOW TABLES", readDB1},
		{"CREATE query", "CREATE TABLE test (id INT)", writeDB},
		{"ALTER query", "ALTER TABLE users ADD COLUMN age INT", writeDB},
		{"TRUNCATE query", "TRUNCATE TABLE users", writeDB},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := rm.DetermineDBForQuery(tc.query)
			assert.Equal(t, tc.expected, db)
		})
	}
}

func TestRoundRobinSelection(t *testing.T) {
	// Setup
	writeDB, _, err := sqlmock.New()
	require.NoError(t, err)

	readDB1, _, err := sqlmock.New()
	require.NoError(t, err)

	readDB2, _, err := sqlmock.New()
	require.NoError(t, err)

	readDB3, _, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB1, readDB2, readDB3},
	}

	// First call should return the first read DB
	assert.Equal(t, readDB1, rm.GetReadDB())

	// Second call should return the second read DB
	assert.Equal(t, readDB2, rm.GetReadDB())

	// Third call should return the third read DB
	assert.Equal(t, readDB3, rm.GetReadDB())

	// Fourth call should wrap around to the first read DB
	assert.Equal(t, readDB1, rm.GetReadDB())
}

func TestExecAndQuery(t *testing.T) {
	// Setup
	writeDB, writeMock, err := sqlmock.New()
	require.NoError(t, err)

	readDB, readMock, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB},
	}

	t.Run("Exec should use write DB for updates", func(t *testing.T) {
		query := "UPDATE users SET name = ?"
		args := []interface{}{"test"}

		// Set expectation on write DB
		writeMock.ExpectExec(query).WithArgs([]driver.Value{"test"}).WillReturnResult(sqlmock.NewResult(0, 1))

		// Call Exec
		result, err := rm.Exec(query, args...)
		assert.NoError(t, err)

		// Verify result
		rowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		// Verify expectations
		assert.NoError(t, writeMock.ExpectationsWereMet())
	})

	t.Run("Query should use read DB for selects", func(t *testing.T) {
		query := "SELECT * FROM users"

		// Set expectation on read DB
		readMock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "user1").
			AddRow(2, "user2"))

		// Call Query
		rows, err := rm.Query(query)
		assert.NoError(t, err)

		// Verify result
		var id int
		var name string
		assert.True(t, rows.Next())
		assert.NoError(t, rows.Scan(&id, &name))
		assert.Equal(t, 1, id)
		assert.Equal(t, "user1", name)

		assert.True(t, rows.Next())
		assert.NoError(t, rows.Scan(&id, &name))
		assert.Equal(t, 2, id)
		assert.Equal(t, "user2", name)

		assert.False(t, rows.Next())
		assert.NoError(t, rows.Close())

		// Verify expectations
		assert.NoError(t, readMock.ExpectationsWereMet())
	})
}

func TestPreparedStatements(t *testing.T) {
	// Setup
	writeDB, writeMock, err := sqlmock.New()
	require.NoError(t, err)

	readDB, readMock, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB},
	}

	t.Run("Prepare and execute SELECT", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = ?"
		args := []interface{}{1}

		// Set expectations for prepare
		writeMock.ExpectPrepare(query)
		readMock.ExpectPrepare(query)

		// Set expectation for query execution
		readMock.ExpectQuery(query).WithArgs([]driver.Value{1}).WillReturnRows(
			sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "user1"))

		// Prepare statement
		stmt, err := rm.Prepare(query)
		assert.NoError(t, err)

		// Execute query
		rows, err := stmt.Query(args...)
		assert.NoError(t, err)

		// Verify result
		var id int
		var name string
		assert.True(t, rows.Next())
		assert.NoError(t, rows.Scan(&id, &name))
		assert.Equal(t, 1, id)
		assert.Equal(t, "user1", name)

		assert.False(t, rows.Next())
		assert.NoError(t, rows.Close())

		// Clean up
		writeMock.ExpectClose()
		readMock.ExpectClose()
		stmt.Close()

		// Verify expectations
		assert.NoError(t, writeMock.ExpectationsWereMet())
		assert.NoError(t, readMock.ExpectationsWereMet())
	})

	t.Run("Prepare and execute UPDATE", func(t *testing.T) {
		query := "UPDATE users SET name = ? WHERE id = ?"
		args := []interface{}{"newname", 1}

		// Set expectations for prepare
		writeMock.ExpectPrepare(query)
		readMock.ExpectPrepare(query)

		// Set expectation for exec
		writeMock.ExpectExec(query).WithArgs([]driver.Value{"newname", 1}).WillReturnResult(
			sqlmock.NewResult(0, 1))

		// Prepare statement
		stmt, err := rm.Prepare(query)
		assert.NoError(t, err)

		// Execute statement
		result, err := stmt.Exec(args...)
		assert.NoError(t, err)

		// Verify result
		rowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)

		// Clean up
		writeMock.ExpectClose()
		readMock.ExpectClose()
		stmt.Close()

		// Verify expectations
		assert.NoError(t, writeMock.ExpectationsWereMet())
		assert.NoError(t, readMock.ExpectationsWereMet())
	})
}

func TestTransactions(t *testing.T) {
	// Setup
	writeDB, writeMock, err := sqlmock.New()
	require.NoError(t, err)

	readDB, _, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB},
	}

	t.Run("Begin and commit transaction", func(t *testing.T) {
		// Set expectations
		writeMock.ExpectBegin()
		writeMock.ExpectExec("UPDATE users SET name = ?").
			WithArgs("newname").
			WillReturnResult(sqlmock.NewResult(0, 1))
		writeMock.ExpectCommit()

		// Begin transaction
		tx, err := rm.Begin()
		assert.NoError(t, err)

		// Execute in transaction
		_, err = tx.Exec("UPDATE users SET name = ?", "newname")
		assert.NoError(t, err)

		// Commit
		err = tx.Commit()
		assert.NoError(t, err)

		// Verify expectations
		assert.NoError(t, writeMock.ExpectationsWereMet())
	})

	t.Run("Begin and rollback transaction", func(t *testing.T) {
		// Set expectations
		writeMock.ExpectBegin()
		writeMock.ExpectExec("UPDATE users SET name = ?").
			WithArgs("newname").
			WillReturnResult(sqlmock.NewResult(0, 1))
		writeMock.ExpectRollback()

		// Begin transaction
		tx, err := rm.Begin()
		assert.NoError(t, err)

		// Execute in transaction
		_, err = tx.Exec("UPDATE users SET name = ?", "newname")
		assert.NoError(t, err)

		// Rollback
		err = tx.Rollback()
		assert.NoError(t, err)

		// Verify expectations
		assert.NoError(t, writeMock.ExpectationsWereMet())
	})
}

func TestContextFunctions(t *testing.T) {
	// Setup
	writeDB, writeMock, err := sqlmock.New()
	require.NoError(t, err)

	readDB, readMock, err := sqlmock.New()
	require.NoError(t, err)

	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB},
	}

	ctx := context.Background()

	t.Run("QueryContext should use read DB", func(t *testing.T) {
		query := "SELECT * FROM users"

		// Set expectation
		readMock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"id"}).AddRow(1))

		// Execute
		rows, err := rm.QueryContext(ctx, query)
		assert.NoError(t, err)

		// Verify
		assert.True(t, rows.Next())
		var id int
		assert.NoError(t, rows.Scan(&id))
		assert.Equal(t, 1, id)

		assert.NoError(t, rows.Close())
		assert.NoError(t, readMock.ExpectationsWereMet())
	})

	t.Run("ExecContext should use write DB", func(t *testing.T) {
		query := "DELETE FROM users"

		// Set expectation
		writeMock.ExpectExec(query).WillReturnResult(
			sqlmock.NewResult(0, 5))

		// Execute
		result, err := rm.ExecContext(ctx, query)
		assert.NoError(t, err)

		// Verify
		rowsAffected, err := result.RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(5), rowsAffected)

		assert.NoError(t, writeMock.ExpectationsWereMet())
	})
}

func TestHealthChecker(t *testing.T) {
	// Create mock DBs
	writeDB, writeMock, err := sqlmock.New()
	require.NoError(t, err)

	readDB1, readMock1, err := sqlmock.New()
	require.NoError(t, err)

	readDB2, readMock2, err := sqlmock.New()
	require.NoError(t, err)

	// Create replica manager
	rm := &dbreplica.ReplicaManager{
		WriteDB: writeDB,
		ReadDBs: []*sql.DB{readDB1, readDB2},
	}

	// Set up expectations for health check
	writeMock.ExpectPing()
	readMock1.ExpectPing()
	readMock2.ExpectPing()

	// Create health checker with very short interval for testing
	hc := dbreplica.NewHealthChecker(rm, 50*time.Millisecond)

	// Start health checker
	hc.Start()

	// Wait for at least one health check to complete
	time.Sleep(100 * time.Millisecond)

	// Stop health checker
	hc.Stop()

	// Verify expectations were met
	assert.NoError(t, writeMock.ExpectationsWereMet())
	assert.NoError(t, readMock1.ExpectationsWereMet())
	assert.NoError(t, readMock2.ExpectationsWereMet())
}

// Add a variable to override sql.Open for testing
var sqlOpen = sql.Open
