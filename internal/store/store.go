package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/panuinc/eis-sync/internal/model"
)

// Store handles all database operations
type Store struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New creates a new Store with connection pool
func New(ctx context.Context, databaseURL string, logger *slog.Logger) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	poolCfg.MaxConns = 8
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	// Disable prepared statements for transaction pooler (PgBouncer)
	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Store{pool: pool, logger: logger}, nil
}

// Close closes the connection pool
func (s *Store) Close() {
	s.pool.Close()
}

// LoadConfigs loads all enabled sync configs
func (s *Store) LoadConfigs(ctx context.Context) ([]model.SyncConfig, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sys_sync_configs_id, sys_sync_configs_company_key, sys_sync_configs_company_id,
			sys_sync_configs_endpoint, sys_sync_configs_table_name, sys_sync_configs_expand,
			sys_sync_configs_is_enabled, sys_sync_configs_last_sync, sys_sync_configs_record_count
		FROM sys_sync_configs
		WHERE sys_sync_configs_is_enabled = true AND sys_sync_configs_is_active = true
		ORDER BY sys_sync_configs_company_key, sys_sync_configs_endpoint
	`)
	if err != nil {
		return nil, fmt.Errorf("query configs: %w", err)
	}
	defer rows.Close()

	var configs []model.SyncConfig
	for rows.Next() {
		var c model.SyncConfig
		if err := rows.Scan(
			&c.ID, &c.CompanyKey, &c.CompanyID,
			&c.Endpoint, &c.TableName, &c.Expand,
			&c.IsEnabled, &c.LastSync, &c.RecordCount,
		); err != nil {
			return nil, fmt.Errorf("scan config: %w", err)
		}
		configs = append(configs, c)
	}

	return configs, nil
}

// UpsertRecords upserts BC records into a mirror table
func (s *Store) UpsertRecords(ctx context.Context, tableName string, records []map[string]any) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	batch := &pgx.Batch{}
	sql := fmt.Sprintf(`
		INSERT INTO %s (bc_id, bc_data, bc_modified_at, bc_synced_at)
		VALUES ($1, $2::jsonb, $3, NOW())
		ON CONFLICT (bc_id)
		DO UPDATE SET bc_data = EXCLUDED.bc_data, bc_modified_at = EXCLUDED.bc_modified_at, bc_synced_at = NOW()
		WHERE %s.bc_data IS DISTINCT FROM EXCLUDED.bc_data
	`, tableName, tableName)

	for _, record := range records {
		id, ok := record["id"].(string)
		if !ok || id == "" {
			continue
		}

		data, err := json.Marshal(record)
		if err != nil {
			s.logger.Warn("marshal record failed", "table", tableName, "id", id, "error", err)
			continue
		}

		var modifiedAt *time.Time
		if ts, ok := record["lastModifiedDateTime"].(string); ok && ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				modifiedAt = &t
			}
		}

		batch.Queue(sql, id, string(data), modifiedAt)
	}

	if batch.Len() == 0 {
		return 0, nil
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	upserted := 0
	failures := 0
	var lastErr error
	for i := 0; i < batch.Len(); i++ {
		ct, err := br.Exec()
		if err != nil {
			failures++
			lastErr = err
			if failures <= 3 {
				s.logger.Warn("upsert failed", "table", tableName, "error", err)
			}
			continue
		}
		upserted += int(ct.RowsAffected())
	}

	if failures > 3 {
		s.logger.Warn("upsert failures summary", "table", tableName, "total_failures", failures)
	}

	// If ALL records failed, return error
	if upserted == 0 && failures > 0 {
		return 0, fmt.Errorf("all %d upserts failed, last error: %w", failures, lastErr)
	}

	return upserted, nil
}

// ClearTable deletes all records from a mirror table (for full sync)
func (s *Store) ClearTable(ctx context.Context, tableName string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s", tableName))
	return err
}

// UpdateSyncConfig updates the last sync time and record count
func (s *Store) UpdateSyncConfig(ctx context.Context, configID string, lastSync time.Time, recordCount int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE sys_sync_configs
		SET sys_sync_configs_last_sync = $1, sys_sync_configs_record_count = $2
		WHERE sys_sync_configs_id = $3
	`, lastSync, recordCount, configID)
	return err
}

// GetTableCount returns the number of records in a mirror table
func (s *Store) GetTableCount(ctx context.Context, tableName string) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	return count, err
}

// GetTableIDs returns all bc_id values in a mirror table as a set
func (s *Store) GetTableIDs(ctx context.Context, tableName string) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf("SELECT bc_id FROM %s", tableName))
	if err != nil {
		return nil, fmt.Errorf("query ids: %w", err)
	}
	defer rows.Close()

	ids := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		ids[id] = struct{}{}
	}
	return ids, nil
}

// DeleteByIDs deletes records from a mirror table by bc_id
func (s *Store) DeleteByIDs(ctx context.Context, tableName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for i := 0; i < len(ids); i += 500 {
		end := i + 500
		if end > len(ids) {
			end = len(ids)
		}
		_, err := s.pool.Exec(ctx,
			fmt.Sprintf("DELETE FROM %s WHERE bc_id = ANY($1::uuid[])", tableName),
			ids[i:end],
		)
		if err != nil {
			return fmt.Errorf("delete batch at %d: %w", i, err)
		}
	}
	return nil
}

// CreateSyncLog creates a sync log entry
func (s *Store) CreateSyncLog(ctx context.Context, configID, syncType, status string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		INSERT INTO sys_sync_logs (sys_sync_configs_id, sys_sync_logs_type, sys_sync_logs_status)
		VALUES ($1, $2, $3)
		RETURNING sys_sync_logs_id
	`, configID, syncType, status).Scan(&id)
	return id, err
}

// CompleteSyncLog updates a sync log entry
func (s *Store) CompleteSyncLog(ctx context.Context, logID, status string, records int, durationMs int64, syncErr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE sys_sync_logs
		SET sys_sync_logs_status = $1, sys_sync_logs_records = $2,
			sys_sync_logs_duration_ms = $3, sys_sync_logs_error = $4
		WHERE sys_sync_logs_id = $5
	`, status, records, durationMs, nullIfEmpty(syncErr), logID)
	return err
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
