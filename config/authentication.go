package config

// Authentication configuration
type Authentication struct {
	OIDC OIDC `json:"oidc" yaml:"oidc"`
}

// OIDC configures the OIDC middleware
type OIDC struct {
	Enabled                bool   `json:"enabled" yaml:"enabled"`
	Issuer                 string `json:"issuer" yaml:"issuer"`
	ClientID               string `json:"client_id" yaml:"client_id"`
	SkipAudienceValidation bool   `json:"skip_audience_validation" yaml:"skip_audience_validation"`
	UsernameClaim          string `yaml:"username_claim" json:"username_claim"`
	GroupsClaim            string `yaml:"groups_claim" json:"groups_claim"`
}
