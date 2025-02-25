package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/emilpriver/dbreplica"
	_ "github.com/lib/pq" // Import your database driver
)

func main() {
	// Connection strings for your databases
	writeConnString := "host=primary.example.com dbname=mydb user=user password=pass"
	readConnStrings := []string{
		"host=replica1.example.com dbname=mydb user=user password=pass",
		"host=replica2.example.com dbname=mydb user=user password=pass",
		"host=replica3.example.com dbname=mydb user=user password=pass",
	}

	// Create a configuration with custom health check interval
	config := dbreplica.DefaultConfig()
	config.HealthCheckInterval = 15 * time.Second

	// Create a new replica manager
	rm, err := dbreplica.NewReplicaManager(writeConnString, readConnStrings, "postgres", config)
	if err != nil {
		log.Fatalf("Failed to create replica manager: %v", err)
	}
	defer rm.Close()

	// Example: Execute a SELECT query that will use a read replica
	rows, err := rm.Query("SELECT id, name FROM users LIMIT 10")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	fmt.Println("User data:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("ID: %d, Name: %s\n", id, name)
	}

	// Example: Execute an INSERT query that will use the write database
	result, err := rm.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "New User", "newuser@example.com")
	if err != nil {
		log.Fatalf("Insert failed: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	fmt.Printf("Inserted %d row(s)\n", rowsAffected)

	// Example: Using transactions (always on write database)
	tx, err := rm.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Perform operations within the transaction
	_, err = tx.Exec("UPDATE users SET status = $1 WHERE id = $2", "active", 1)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to update within transaction: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	// Example: Using a prepared statement
	stmt, err := rm.Prepare("SELECT name FROM users WHERE id = $1")
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Using the prepared statement (will use read replica for this query)
	var name string
	err = stmt.QueryRow(1).Scan(&name)
	if err != nil {
		log.Fatalf("Failed to query with prepared statement: %v", err)
	}
	fmt.Printf("User with ID 1 is named: %s\n", name)

	// Example: Using context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err = rm.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to count users: %v", err)
	}
	fmt.Printf("Total users: %d\n", count)
}
