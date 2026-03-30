package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgresql://postgres:AXd5oH4dQxDPdrQm@db.wpolyxvozxdblapddufk.supabase.co:6543/postgres"
	}

	ctx := context.Background()
	config, _ := pgx.ParseConfig(dbURL)
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND (tablename LIKE 'sys_%' OR tablename LIKE 'sc_%' OR tablename LIKE 'hr_%' OR tablename LIKE 'it_%' OR tablename LIKE 'mk_%') ORDER BY tablename")
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	defer rows.Close()

	fmt.Println("=== EIS Tables in Production ===")
	count := 0
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Println(name)
		count++
	}
	fmt.Printf("\nTotal: %d tables\n", count)

	// Check auth.users
	var userCount int
	conn.QueryRow(ctx, "SELECT count(*) FROM auth.users").Scan(&userCount)
	fmt.Printf("Auth users: %d\n", userCount)

	// Check if trigger function exists
	var fnCount int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_proc WHERE proname = 'trigger_set_updated_at'").Scan(&fnCount)
	fmt.Printf("trigger_set_updated_at: %s\n", map[bool]string{true: "exists", false: "MISSING"}[fnCount > 0])

	var handleNewUser int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_proc WHERE proname = 'handle_new_user'").Scan(&handleNewUser)
	fmt.Printf("handle_new_user: %s\n", map[bool]string{true: "exists", false: "MISSING"}[handleNewUser > 0])
}
