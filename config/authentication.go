package config

// Authentication configuration
type Authentication struct {
	OIDC OIDC `json:"oidc" yaml:"oidc"`
}

// OIDC configures the OIDC middleware
type OIDC struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}
