package bc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"time"

	"github.com/panuinc/eis-sync/internal/config"
	"github.com/panuinc/eis-sync/internal/model"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/time/rate"
)

// Client is the Business Central API client
type Client struct {
	httpClient *http.Client
	limiter    *rate.Limiter
	cfg        *config.Config
	logger     *slog.Logger
}

// NewClient creates a new BC API client with OAuth2 and rate limiting
func NewClient(cfg *config.Config, logger *slog.Logger) *Client {
	// Custom transport with connection pooling
	transport := &http.Transport{
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:  10 * time.Second,
		ResponseHeaderTimeout: 0, // no timeout — BC API can be slow on large $skip
	}

	baseClient := &http.Client{
		Transport: transport,
		Timeout:   0, // no timeout — rely on context cancellation for shutdown
	}

	// OAuth2 client credentials
	oauthCfg := &clientcredentials.Config{
		ClientID:     cfg.BCClientID,
		ClientSecret: cfg.BCClientSecret,
		TokenURL:     cfg.BCTokenURL(),
		Scopes:       []string{"https://api.businesscentral.dynamics.com/.default"},
	}

	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, baseClient)
	httpClient := oauthCfg.Client(ctx)

	// Rate limiter: 8 requests/second with burst of 10
	limiter := rate.NewLimiter(rate.Every(125*time.Millisecond), 10)

	return &Client{
		httpClient: httpClient,
		limiter:    limiter,
		cfg:        cfg,
		logger:     logger,
	}
}

// FetchPage fetches a single page of data from a BC endpoint
func (c *Client) FetchPage(ctx context.Context, companyID, endpoint string, top, skip int, filter string) (*model.ODataResponse, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	baseURL := c.cfg.BCBaseURL(companyID)
	url := fmt.Sprintf("%s/%s?$top=%d&$skip=%d", baseURL, endpoint, top, skip)

	if filter != "" {
		url += "&$filter=" + neturl.QueryEscape(filter)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode == 429 {
		retryAfter := resp.Header.Get("Retry-After")
		return nil, fmt.Errorf("rate limited (429), retry after: %s", retryAfter)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("BC API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
	}

	var result model.ODataResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result, nil
}

// FetchAll fetches all data from a BC endpoint using createdAt date chunks.
// Splits into quarterly chunks to keep $skip low and avoid BC API timeouts.
func (c *Client) FetchAll(ctx context.Context, companyID, endpoint string) ([]map[string]any, error) {
	if c.cfg.TestMode {
		page, err := c.FetchPage(ctx, companyID, endpoint, c.cfg.PageSize, 0, "")
		if err != nil {
			return nil, fmt.Errorf("test fetch: %w", err)
		}
		return page.Value, nil
	}

	var all []map[string]any
	seen := make(map[string]bool)
	now := time.Now()
	startYear := 2018

	for year := startYear; year <= now.Year(); year++ {
		quarters := []struct{ from, to string }{
			{fmt.Sprintf("%d-01-01", year), fmt.Sprintf("%d-04-01", year)},
			{fmt.Sprintf("%d-04-01", year), fmt.Sprintf("%d-07-01", year)},
			{fmt.Sprintf("%d-07-01", year), fmt.Sprintf("%d-10-01", year)},
			{fmt.Sprintf("%d-10-01", year), fmt.Sprintf("%d-01-01", year+1)},
		}

		for _, q := range quarters {
			filter := fmt.Sprintf("createdAt ge %sT00:00:00Z and createdAt lt %sT00:00:00Z", q.from, q.to)
			skip := 0

			for {
				page, err := c.FetchPage(ctx, companyID, endpoint, c.cfg.PageSize, skip, filter)
				if err != nil {
					return all, fmt.Errorf("chunk %s~%s skip %d: %w", q.from, q.to, skip, err)
				}

				if len(page.Value) == 0 {
					break
				}

				for _, r := range page.Value {
					if id, ok := r["id"].(string); ok && !seen[id] {
						all = append(all, r)
						seen[id] = true
					}
				}

				skip += c.cfg.PageSize
				if len(page.Value) < c.cfg.PageSize {
					break
				}
			}
		}
	}

	return all, nil
}

// FetchAllIDs fetches only the IDs of all records from a BC endpoint.
// Used for deletion detection in incremental sync.
func (c *Client) FetchAllIDs(ctx context.Context, companyID, endpoint string) (map[string]struct{}, error) {
	const idPageSize = 1000
	ids := make(map[string]struct{})
	skip := 0

	for {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limiter: %w", err)
		}

		baseURL := c.cfg.BCBaseURL(companyID)
		url := fmt.Sprintf("%s/%s?$top=%d&$skip=%d&$select=id", baseURL, endpoint, idPageSize, skip)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("http request: %w", err)
		}

		if resp.StatusCode == 429 {
			retryAfter := resp.Header.Get("Retry-After")
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("rate limited (429), retry after: %s", retryAfter)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("BC API %d: %s", resp.StatusCode, string(body[:min(len(body), 200)]))
		}

		var result model.ODataResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("decode response: %w", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		for _, r := range result.Value {
			if id, ok := r["id"].(string); ok && id != "" {
				ids[id] = struct{}{}
			}
		}

		if len(result.Value) < idPageSize {
			break
		}
		skip += idPageSize
	}

	return ids, nil
}

// FetchIncremental fetches records modified after the given time
func (c *Client) FetchIncremental(ctx context.Context, companyID, endpoint string, since time.Time) ([]map[string]any, error) {
	var all []map[string]any
	skip := 0
	pageSize := c.cfg.PageSize
	filter := fmt.Sprintf("lastModifiedDateTime gt %s", since.UTC().Format(time.RFC3339))

	for {
		page, err := c.FetchPage(ctx, companyID, endpoint, pageSize, skip, filter)
		if err != nil {
			return all, fmt.Errorf("page %d: %w", skip/pageSize+1, err)
		}

		if len(page.Value) == 0 {
			break
		}

		all = append(all, page.Value...)
		skip += pageSize

		if len(page.Value) < pageSize {
			break
		}
	}

	return all, nil
}
