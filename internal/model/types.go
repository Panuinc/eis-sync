package model

import "time"

// SyncConfig represents a row from sys_sync_configs
type SyncConfig struct {
	ID          string     `db:"sys_sync_configs_id"`
	CompanyKey  string     `db:"sys_sync_configs_company_key"`
	CompanyID   string     `db:"sys_sync_configs_company_id"`
	Endpoint    string     `db:"sys_sync_configs_endpoint"`
	TableName   string     `db:"sys_sync_configs_table_name"`
	Expand      *string    `db:"sys_sync_configs_expand"`
	IsEnabled   bool       `db:"sys_sync_configs_is_enabled"`
	LastSync    *time.Time `db:"sys_sync_configs_last_sync"`
	RecordCount int        `db:"sys_sync_configs_record_count"`
}

// SyncResult represents the outcome of a sync operation
type SyncResult struct {
	ConfigID   string
	Company    string
	Endpoint   string
	Type       string // "full" or "incremental"
	Records    int
	DurationMs int64
	Error      error
}

// ODataResponse is the generic BC API response
type ODataResponse struct {
	Value    []map[string]any `json:"value"`
	NextLink string           `json:"@odata.nextLink,omitempty"`
	Count    int              `json:"@odata.count,omitempty"`
}
