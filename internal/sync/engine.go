package sync

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/panuinc/eis-sync/internal/bc"
	"github.com/panuinc/eis-sync/internal/config"
	"github.com/panuinc/eis-sync/internal/model"
	"github.com/panuinc/eis-sync/internal/store"
	"golang.org/x/sync/errgroup"
)

// deletionDetectionEndpoints lists endpoints where records can disappear
// (non-posted documents that move to posted tables when confirmed in BC)
var deletionDetectionEndpoints = map[string]bool{
	"salesQuotes":              true,
	"salesQuoteLines":          true,
	"salesOrders":              true,
	"salesOrderLines":          true,
	"salesInvoices":            true,
	"salesInvoiceLines":        true,
	"salesCreditMemos":         true,
	"salesCreditMemoLines":     true,
	"purchaseOrders":           true,
	"purchaseOrderLines":       true,
	"purchaseInvoices":         true,
	"purchaseInvoiceLines":     true,
	"purchaseCreditMemos":      true,
	"purchaseCreditMemoLines":  true,
	"productionOrders":         true,
	"productionOrderLines":     true,
	"productionOrderComponents": true,
}

// Engine orchestrates the sync process
type Engine struct {
	cfg        *config.Config
	bcClient   *bc.Client
	store      *store.Store
	logger     *slog.Logger
	syncActive atomic.Bool
}

// NewEngine creates a new sync engine
func NewEngine(cfg *config.Config, bcClient *bc.Client, store *store.Store, logger *slog.Logger) *Engine {
	return &Engine{
		cfg:      cfg,
		bcClient: bcClient,
		store:    store,
		logger:   logger,
	}
}

// Run starts the sync engine with scheduler
func (e *Engine) Run(ctx context.Context) error {
	e.logger.Info("sync engine starting",
		"interval", e.cfg.SyncInterval,
		"concurrency", e.cfg.MaxConcurrency,
	)

	// Run initial incremental sync
	e.runSync(ctx, "incremental")

	// Start scheduler
	ticker := time.NewTicker(e.cfg.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("sync engine shutting down")
			return ctx.Err()
		case <-ticker.C:
			e.runSync(ctx, "incremental")
		}
	}
}

// FullSync runs a full sync for all endpoints
func (e *Engine) FullSync(ctx context.Context) {
	e.runSync(ctx, "full")
}

func (e *Engine) runSync(ctx context.Context, syncType string) {
	if !e.syncActive.CompareAndSwap(false, true) {
		e.logger.Warn("skipping sync, previous cycle still running")
		return
	}
	defer e.syncActive.Store(false)

	start := time.Now()

	configs, err := e.store.LoadConfigs(ctx)
	if err != nil {
		e.logger.Error("failed to load configs", "error", err)
		return
	}

	e.logger.Info("sync cycle started",
		"type", syncType,
		"endpoints", len(configs),
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(e.cfg.MaxConcurrency)

	var success, failed atomic.Int32

	for _, cfg := range configs {
		cfg := cfg // capture
		g.Go(func() error {
			result := e.syncEndpoint(gCtx, cfg, syncType)
			if result.Error != nil {
				failed.Add(1)
				e.logger.Error("sync failed",
					"company", result.Company,
					"endpoint", result.Endpoint,
					"error", result.Error,
					"duration_ms", result.DurationMs,
				)
			} else {
				success.Add(1)
				e.logger.Info("sync ok",
					"company", result.Company,
					"endpoint", result.Endpoint,
					"records", result.Records,
					"duration_ms", result.DurationMs,
				)
			}
			return nil // don't cancel other workers on failure
		})
	}

	g.Wait()

	e.logger.Info("sync cycle complete",
		"type", syncType,
		"total", len(configs),
		"success", success.Load(),
		"failed", failed.Load(),
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

func (e *Engine) syncEndpoint(ctx context.Context, cfg model.SyncConfig, syncType string) model.SyncResult {
	start := time.Now()
	result := model.SyncResult{
		ConfigID: cfg.ID,
		Company:  cfg.CompanyKey,
		Endpoint: cfg.Endpoint,
		Type:     syncType,
	}

	// Create sync log
	logID, err := e.store.CreateSyncLog(ctx, cfg.ID, syncType, "running")
	if err != nil {
		result.Error = fmt.Errorf("create log: %w", err)
		return result
	}

	defer func() {
		result.DurationMs = time.Since(start).Milliseconds()
		errMsg := ""
		status := "success"
		if result.Error != nil {
			errMsg = result.Error.Error()
			status = "failed"
		}
		e.store.CompleteSyncLog(ctx, logID, status, result.Records, result.DurationMs, errMsg)
	}()

	var records []map[string]any

	if syncType == "full" {
		// Full sync: clear + fetch all
		if err := e.store.ClearTable(ctx, cfg.TableName); err != nil {
			result.Error = fmt.Errorf("clear table: %w", err)
			return result
		}

		records, err = e.bcClient.FetchAll(ctx, cfg.CompanyID, cfg.Endpoint)
		if err != nil {
			result.Error = fmt.Errorf("fetch all: %w", err)
			return result
		}
	} else {
		// Incremental sync
		since := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		if cfg.LastSync != nil {
			since = *cfg.LastSync
		}

		records, err = e.bcClient.FetchIncremental(ctx, cfg.CompanyID, cfg.Endpoint, since)
		if err != nil {
			result.Error = fmt.Errorf("fetch incremental: %w", err)
			return result
		}
	}

	// Upsert records
	if len(records) > 0 {
		// Batch in chunks of 500
		for i := 0; i < len(records); i += 500 {
			end := i + 500
			if end > len(records) {
				end = len(records)
			}
			if _, err := e.store.UpsertRecords(ctx, cfg.TableName, records[i:end]); err != nil {
				result.Error = fmt.Errorf("upsert batch %d: %w", i/500, err)
				return result
			}
		}
	}

	result.Records = len(records)

	// Deletion detection (incremental only): remove records no longer in BC
	// Only run for non-posted documents that can disappear when posted/converted
	if syncType == "incremental" && deletionDetectionEndpoints[cfg.Endpoint] {
		bcIDs, err := e.bcClient.FetchAllIDs(ctx, cfg.CompanyID, cfg.Endpoint)
		if err != nil {
			e.logger.Warn("deletion detection: fetch BC IDs failed, skipping",
				"table", cfg.TableName, "error", err)
		} else {
			dbIDs, err := e.store.GetTableIDs(ctx, cfg.TableName)
			if err != nil {
				e.logger.Warn("deletion detection: get table IDs failed, skipping",
					"table", cfg.TableName, "error", err)
			} else {
				var toDelete []string
				for id := range dbIDs {
					if _, exists := bcIDs[id]; !exists {
						toDelete = append(toDelete, id)
					}
				}
				if len(toDelete) > 0 {
					if err := e.store.DeleteByIDs(ctx, cfg.TableName, toDelete); err != nil {
						e.logger.Warn("deletion detection: delete failed",
							"table", cfg.TableName, "count", len(toDelete), "error", err)
					} else {
						e.logger.Info("deletion detection: orphans removed",
							"table", cfg.TableName, "count", len(toDelete))
					}
				}
			}
		}
	}

	// Verify actual count in DB before updating config
	count, _ := e.store.GetTableCount(ctx, cfg.TableName)
	if syncType == "full" && len(records) > 0 && count == 0 {
		result.Error = fmt.Errorf("full sync fetched %d records but 0 in DB — upsert likely failed", len(records))
		return result
	}

	// Update config only on success
	now := time.Now()
	e.store.UpdateSyncConfig(ctx, cfg.ID, now, count)

	return result
}
