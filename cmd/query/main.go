package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func main() {
	dbURL := "postgresql://postgres:AXd5oH4dQxDPdrQm@db.wpolyxvozxdblapddufk.supabase.co:6543/postgres"
	sqlFile := os.Args[1]

	ctx := context.Background()
	config, _ := pgx.ParseConfig(dbURL)
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	data, _ := os.ReadFile(sqlFile)
	rows, err := conn.Query(ctx, string(data))
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	for _, c := range cols {
		fmt.Printf("%-40s", c.Name)
	}
	fmt.Println()

	for rows.Next() {
		vals, _ := rows.Values()
		for _, v := range vals {
			if v == nil {
				fmt.Printf("%-40s", "-")
			} else {
				fmt.Printf("%-40v", v)
			}
		}
		fmt.Println()
	}
}
