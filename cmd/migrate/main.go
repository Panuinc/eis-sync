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

	sqlFile := `E:\tmp\fix_production.sql`
	if len(os.Args) > 1 {
		sqlFile = os.Args[1]
	}

	ctx := context.Background()
	config, _ := pgx.ParseConfig(dbURL)
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	fmt.Println("Connected!")

	data, err := os.ReadFile(sqlFile)
	if err != nil {
		log.Fatalf("read: %v", err)
	}

	fmt.Printf("Running %s (%d bytes)...\n", sqlFile, len(data))
	_, err = conn.Exec(ctx, string(data))
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
	} else {
		fmt.Println("OK!")
	}

	// Verify
	var tables int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND (tablename LIKE 'sys_%' OR tablename LIKE 'sc_%' OR tablename LIKE 'hr_%' OR tablename LIKE 'it_%')").Scan(&tables)
	fmt.Printf("EIS tables: %d\n", tables)

	var roles int
	conn.QueryRow(ctx, "SELECT count(*) FROM sc_roles").Scan(&roles)
	fmt.Printf("Roles: %d\n", roles)

	var perms int
	conn.QueryRow(ctx, "SELECT count(*) FROM sc_permissions").Scan(&perms)
	fmt.Printf("Permissions: %d\n", perms)

	var users int
	conn.QueryRow(ctx, "SELECT count(*) FROM sys_users").Scan(&users)
	fmt.Printf("Sys users: %d\n", users)

	var fn int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_proc WHERE proname = 'trigger_set_updated_at'").Scan(&fn)
	fmt.Printf("trigger_set_updated_at: %v\n", fn > 0)
}
