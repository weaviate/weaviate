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

import (
	"fmt"

	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// Authentication configuration
type Authentication struct {
	OIDC            OIDC            `json:"oidc" yaml:"oidc"`
	AnonymousAccess AnonymousAccess `json:"anonymous_access" yaml:"anonymous_access"`
	APIKey          StaticAPIKey    // don't change name to not break yaml files
	DBUsers         DbUsers         `json:"db_users" yaml:"db_users"`
}

// DefaultAuthentication is the default authentication scheme when no authentication is provided
var DefaultAuthentication = Authentication{
	AnonymousAccess: AnonymousAccess{
		Enabled: true,
	},
}

// Validate the Authentication configuration. This only validates at a general
// level. Validation specific to the individual auth methods should happen
// inside their respective packages
func (a Authentication) Validate() error {
	if !a.AnyAuthMethodSelected() {
		return fmt.Errorf("no authentication scheme configured, you must select at least one")
	}

	return nil
}

func (a Authentication) AnyAuthMethodSelected() bool {
	return a.AnonymousAccess.Enabled || a.OIDC.Enabled || a.APIKey.Enabled || a.DBUsers.Enabled
}

func (a Authentication) AnyApiKeyAvailable() bool {
	return a.APIKey.Enabled || a.DBUsers.Enabled
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
	Enabled           bool                            `json:"enabled" yaml:"enabled"`
	Issuer            *runtime.DynamicValue[string]   `json:"issuer" yaml:"issuer"`
	ClientID          *runtime.DynamicValue[string]   `json:"client_id" yaml:"client_id"`
	SkipClientIDCheck *runtime.DynamicValue[bool]     `yaml:"skip_client_id_check" json:"skip_client_id_check"`
	UsernameClaim     *runtime.DynamicValue[string]   `yaml:"username_claim" json:"username_claim"`
	GroupsClaim       *runtime.DynamicValue[string]   `yaml:"groups_claim" json:"groups_claim"`
	Scopes            *runtime.DynamicValue[[]string] `yaml:"scopes" json:"scopes"`
	Certificate       *runtime.DynamicValue[string]   `yaml:"certificate" json:"certificate"`
}

type StaticAPIKey struct {
	Enabled     bool     `json:"enabled" yaml:"enabled"`
	Users       []string `json:"users" yaml:"users"`
	AllowedKeys []string `json:"allowed_keys" yaml:"allowed_keys"`
}

type DbUsers struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}
