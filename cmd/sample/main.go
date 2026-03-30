package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {
	dbURL := "postgresql://postgres:AXd5oH4dQxDPdrQm@db.wpolyxvozxdblapddufk.supabase.co:6543/postgres"
	ctx := context.Background()
	config, _ := pgx.ParseConfig(dbURL)
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	tables := []struct{ name, key string }{
		{"bc_chh_items", "noValue,description,inventory,unitPrice,unitCost,itemCategoryCode,baseUnitOfMeasure"},
		{"bc_chh_sales_orders", "noValue,sellToCustomerNo,sellToCustomerName,orderDate,status,amount,amountIncludingVAT"},
		{"bc_chh_purchase_orders", "noValue,buyFromVendorNo,buyFromVendorName,orderDate,status,amount,amountIncludingVAT"},
		{"bc_chh_production_orders", "noValue,description,sourceNo,status,quantity,startingDate,endingDate"},
		{"bc_chh_item_ledger_entries", "itemNo,postingDate,entryType,documentNo,description,quantity,remainingQuantity"},
	}

	for _, t := range tables {
		fmt.Printf("\n=== %s ===\n", t.name)
		var data json.RawMessage
		err := conn.QueryRow(ctx, fmt.Sprintf("SELECT bc_data FROM %s LIMIT 1", t.name)).Scan(&data)
		if err != nil {
			fmt.Printf("  error: %v\n", err)
			continue
		}
		var m map[string]any
		json.Unmarshal(data, &m)
		for _, k := range []string{"noValue", "description", "inventory", "unitPrice", "unitCost", "itemCategoryCode", "baseUnitOfMeasure", "sellToCustomerNo", "sellToCustomerName", "orderDate", "status", "amount", "amountIncludingVAT", "buyFromVendorNo", "buyFromVendorName", "sourceNo", "quantity", "startingDate", "endingDate", "itemNo", "postingDate", "entryType", "documentNo", "remainingQuantity", "locationCode"} {
			if v, ok := m[k]; ok {
				fmt.Printf("  %-30s = %v\n", k, v)
			}
		}
	}
}
