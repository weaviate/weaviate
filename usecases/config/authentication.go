//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import "fmt"

// Authentication configuration
type Authentication struct {
	OIDC            OIDC            `json:"oidc" yaml:"oidc"`
	AnonymousAccess AnonymousAccess `json:"anonymous_access" yaml:"anonymous_access"`
	APIKey          APIKey
}

// Validate the Authentication configuration. This only validates at a general
// level. Validation specific to the individual auth methods should happen
// inside their respective packages
func (a Authentication) Validate() error {
	if !a.anyAuthMethodSelected() {
		return fmt.Errorf("no authentication scheme configured, you must select at least one")
	}

	return nil
}

func (a Authentication) anyAuthMethodSelected() bool {
	return a.AnonymousAccess.Enabled || a.OIDC.Enabled || a.APIKey.Enabled
}

// AnonymousAccess considers users without any auth information as
// authenticated as "anonymous" rather than denying their request immediately.
// Note that enabling anonymous access ONLY affects Authentication, not
// Authorization.
type AnonymousAccess struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

// OIDC configures the OIDC middleware
type OIDC struct {
	Enabled           bool     `json:"enabled" yaml:"enabled"`
	Issuer            string   `json:"issuer" yaml:"issuer"`
	ClientID          string   `json:"client_id" yaml:"client_id"`
	SkipClientIDCheck bool     `yaml:"skip_client_id_check" json:"skip_client_id_check"`
	UsernameClaim     string   `yaml:"username_claim" json:"username_claim"`
	GroupsClaim       string   `yaml:"groups_claim" json:"groups_claim"`
	Scopes            []string `yaml:"scopes" json:"scopes"`
}

type APIKey struct {
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	Users       []string `json:"users" yaml:"users"`
	AllowedKeys []string `json:"allowed_keys" yaml:"allowed_keys"`
}
