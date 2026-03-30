package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

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

	fmt.Println("Connected to Production DB!")

	createSyncInfra(ctx, conn)
	seedSyncConfigs(ctx, conn)

	var count int
	conn.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'bc_%'").Scan(&count)
	fmt.Printf("Total bc_* tables: %d\n", count)

	var configCount int
	conn.QueryRow(ctx, "SELECT count(*) FROM sys_sync_configs").Scan(&configCount)
	fmt.Printf("Total sync configs: %d\n", configCount)

	fmt.Println("Done!")
}

func createSyncInfra(ctx context.Context, conn *pgx.Conn) {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS sys_sync_configs (
			sys_sync_configs_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			sys_sync_configs_company_key text NOT NULL,
			sys_sync_configs_company_id text NOT NULL,
			sys_sync_configs_endpoint text NOT NULL,
			sys_sync_configs_table_name text NOT NULL,
			sys_sync_configs_expand text,
			sys_sync_configs_is_enabled boolean NOT NULL DEFAULT true,
			sys_sync_configs_last_sync timestamptz,
			sys_sync_configs_record_count integer NOT NULL DEFAULT 0,
			sys_sync_configs_is_active boolean NOT NULL DEFAULT true,
			sys_sync_configs_created_at timestamptz NOT NULL DEFAULT now(),
			sys_sync_configs_updated_at timestamptz NOT NULL DEFAULT now(),
			UNIQUE (sys_sync_configs_company_key, sys_sync_configs_endpoint)
		)
	`)
	if err != nil {
		log.Printf("create sys_sync_configs: %v", err)
	}

	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS sys_sync_logs (
			sys_sync_logs_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
			sys_sync_configs_id uuid REFERENCES sys_sync_configs(sys_sync_configs_id),
			sys_sync_logs_type text NOT NULL,
			sys_sync_logs_status text NOT NULL,
			sys_sync_logs_records integer NOT NULL DEFAULT 0,
			sys_sync_logs_duration_ms integer,
			sys_sync_logs_error text,
			sys_sync_logs_is_active boolean NOT NULL DEFAULT true,
			sys_sync_logs_created_at timestamptz NOT NULL DEFAULT now(),
			sys_sync_logs_updated_at timestamptz NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		log.Printf("create sys_sync_logs: %v", err)
	}

	conn.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_sys_sync_logs_configs_id ON sys_sync_logs (sys_sync_configs_id)")
	conn.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_sys_sync_logs_created_at ON sys_sync_logs (sys_sync_logs_created_at)")

	fmt.Println("Sync infrastructure ready")
}

func seedSyncConfigs(ctx context.Context, conn *pgx.Conn) {
	type ep struct{ snake, camel, expand string }
	endpoints := []ep{
		{"items", "items", ""}, {"sales_orders", "salesOrders", "salesOrderLines"}, {"sales_order_lines", "salesOrderLines", ""}, {"sales_quotes", "salesQuotes", "salesQuoteLines"}, {"sales_quote_lines", "salesQuoteLines", ""}, {"sales_invoices", "salesInvoices", "salesInvoiceLines"}, {"sales_invoice_lines", "salesInvoiceLines", ""}, {"sales_credit_memos", "salesCreditMemos", "salesCreditMemoLines"}, {"sales_credit_memo_lines", "salesCreditMemoLines", ""}, {"posted_sales_invoices", "postedSalesInvoices", "postedSalesInvoiceLines"}, {"posted_sales_invoice_lines", "postedSalesInvoiceLines", ""}, {"posted_sales_shipments", "postedSalesShipments", "postedSalesShipmentLines"}, {"posted_sales_shipment_lines", "postedSalesShipmentLines", ""}, {"posted_sales_credit_memos", "postedSalesCreditMemos", "postedSalesCreditMemoLines"}, {"posted_sales_credit_memo_lines", "postedSalesCreditMemoLines", ""}, {"purchase_orders", "purchaseOrders", "purchaseOrderLines"}, {"purchase_order_lines", "purchaseOrderLines", ""}, {"purchase_invoices", "purchaseInvoices", "purchaseInvoiceLines"}, {"purchase_invoice_lines", "purchaseInvoiceLines", ""}, {"purchase_credit_memos", "purchaseCreditMemos", "purchaseCreditMemoLines"}, {"purchase_credit_memo_lines", "purchaseCreditMemoLines", ""}, {"posted_purch_invoices", "postedPurchInvoices", "postedPurchInvoiceLines"}, {"posted_purch_invoice_lines", "postedPurchInvoiceLines", ""}, {"posted_purchase_receipts", "postedPurchaseReceipts", "postedPurchaseReceiptLines"}, {"posted_purchase_receipt_lines", "postedPurchaseReceiptLines", ""}, {"posted_purchase_credit_memos", "postedPurchaseCreditMemos", "postedPurchaseCreditMemoLines"}, {"posted_purchase_credit_memo_lines", "postedPurchaseCreditMemoLines", ""}, {"production_orders", "productionOrders", "productionOrderLines"}, {"production_order_lines", "productionOrderLines", ""}, {"production_order_components", "productionOrderComponents", ""}, {"customers", "customers", ""}, {"vendors", "vendors", ""}, {"bank_accounts", "bankAccounts", ""}, {"gl_accounts", "glAccounts", ""}, {"gl_entries", "gLEntries", ""}, {"customer_ledger_entries", "customerLedgerEntries", ""}, {"detailed_cust_ledger_entries", "detailedCustLedgerEntries", ""}, {"vendor_ledger_entries", "vendorLedgerEntries", ""}, {"detailed_vendor_ledger_entries", "detailedVendorLedgerEntries", ""}, {"bank_account_ledger_entries", "bankAccountLedgerEntries", ""}, {"value_entries", "valueEntries", ""}, {"item_ledger_entries", "itemLedgerEntries", ""}, {"fixed_assets", "fixedAssets", ""}, {"fa_ledger_entries", "faLedgerEntries", ""}, {"dimension_set_entries", "dimensionSetEntries", ""}, {"dimension_values", "dimensionValues", ""},
	}
	companies := []struct{ key, id string }{
		{"CHH", "a407ba9f-2151-ec11-9f09-000d3ac85269"},
		{"DXC", "368d58ad-b2ac-ef11-b8ea-6045bd217994"},
		{"WWS", "2622f71a-bfbb-ef11-b8ec-002248589063"},
	}

	inserted := 0
	for _, c := range companies {
		for _, e := range endpoints {
			table := "bc_" + strings.ToLower(c.key) + "_" + e.snake
			var expand *string
			if e.expand != "" {
				expand = &e.expand
			}
			_, err := conn.Exec(ctx, `
				INSERT INTO sys_sync_configs (sys_sync_configs_company_key, sys_sync_configs_company_id, sys_sync_configs_endpoint, sys_sync_configs_table_name, sys_sync_configs_expand)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (sys_sync_configs_company_key, sys_sync_configs_endpoint) DO NOTHING
			`, c.key, c.id, e.camel, table, expand)
			if err != nil {
				log.Printf("insert %s/%s: %v", c.key, e.camel, err)
				continue
			}
			inserted++
		}
	}
	fmt.Printf("Inserted %d sync configs\n", inserted)
}
