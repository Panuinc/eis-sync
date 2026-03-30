package config

import "time"

type Config struct {
	// Business Central
	BCTenantID     string `env:"BC_TENANT_ID,required"`
	BCClientID     string `env:"BC_CLIENT_ID,required"`
	BCClientSecret string `env:"BC_CLIENT_SECRET,required"`
	BCEnvironment  string `env:"BC_ENVIRONMENT" envDefault:"production"`

	// Supabase PostgreSQL (direct connection)
	DatabaseURL string `env:"DATABASE_URL,required"`

	// Sync settings
	SyncInterval   time.Duration `env:"SYNC_INTERVAL" envDefault:"30s"`
	MaxConcurrency int           `env:"MAX_CONCURRENCY" envDefault:"20"`
	PageSize       int           `env:"PAGE_SIZE" envDefault:"100"`
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"30s"`

	// Test mode — fetch only 1 page per endpoint
	TestMode bool `env:"TEST_MODE" envDefault:"false"`

	// Logging
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}

func (c *Config) BCTokenURL() string {
	return "https://login.microsoftonline.com/" + c.BCTenantID + "/oauth2/v2.0/token"
}

func (c *Config) BCBaseURL(companyID string) string {
	return "https://api.businesscentral.dynamics.com/v2.0/" +
		c.BCTenantID + "/" + c.BCEnvironment +
		"/api/evergreen/erp/v1.0/companies(" + companyID + ")"
}
