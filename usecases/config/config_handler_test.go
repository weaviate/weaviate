//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config/runtime"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigModules(t *testing.T) {
	t.Run("invalid DefaultVectorDistanceMetric", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "text2vec-contextionary",
			DefaultVectorDistanceMetric: "euclidean",
		}
		err := config.ValidateModules(moduleProvider)
		assert.EqualError(
			t,
			err,
			"default vector distance metric: must be one of [\"cosine\", \"dot\", \"l2-squared\", \"manhattan\",\"hamming\"]",
		)
	})

	t.Run("invalid DefaultVectorizerModule", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "contextionary",
			DefaultVectorDistanceMetric: "cosine",
		}
		err := config.ValidateModules(moduleProvider)
		assert.EqualError(
			t,
			err,
			"default vectorizer module: invalid vectorizer \"contextionary\"",
		)
	})

	t.Run("all valid configurations", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule:     "text2vec-contextionary",
			DefaultVectorDistanceMetric: "l2-squared",
		}
		err := config.ValidateModules(moduleProvider)
		assert.Nil(t, err, "should not error")
	})

	t.Run("without DefaultVectorDistanceMetric", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule: "text2vec-contextionary",
		}
		err := config.ValidateModules(moduleProvider)
		assert.Nil(t, err, "should not error")
	})

	t.Run("with none DefaultVectorizerModule", func(t *testing.T) {
		moduleProvider := &fakeModuleProvider{
			valid: []string{"text2vec-contextionary"},
		}
		config := Config{
			DefaultVectorizerModule: "none",
		}
		err := config.ValidateModules(moduleProvider)
		assert.Nil(t, err, "should not error")
	})

	t.Run("parse config.yaml file", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authentication:
  apikey:
    enabled: true
    allowed_keys:
      - api-key-1
    users:
      - readonly@weaviate.io`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.APIKey.Enabled)
		assert.ElementsMatch(t, []string{"api-key-1"}, config.Authentication.APIKey.AllowedKeys)
		assert.ElementsMatch(t, []string{"readonly@weaviate.io"}, config.Authentication.APIKey.Users)
	})
}

func TestConfigParsing(t *testing.T) {
	t.Run("parse config.yaml with oidc config - yaml", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authentication:
  oidc:
    enabled: true
    issuer: http://localhost:9090/auth/realms/weaviate
    username_claim: preferred_username
    groups_claim: groups
    client_id: demo
    skip_client_id_check: false
    scopes: ['email', 'openid']
    certificate: "valid-certificate"
`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.OIDC.Enabled)
		assert.Equal(t, "http://localhost:9090/auth/realms/weaviate", config.Authentication.OIDC.Issuer.Get())
		assert.Equal(t, "preferred_username", config.Authentication.OIDC.UsernameClaim.Get())
		assert.Equal(t, "groups", config.Authentication.OIDC.GroupsClaim.Get())
		assert.Equal(t, "demo", config.Authentication.OIDC.ClientID.Get())
		assert.False(t, config.Authentication.OIDC.SkipClientIDCheck.Get())
		assert.ElementsMatch(t, []string{"email", "openid"}, config.Authentication.OIDC.Scopes.Get())
		assert.Equal(t, "valid-certificate", config.Authentication.OIDC.Certificate.Get())
	})

	t.Run("parse config.yaml with oidc config - json", func(t *testing.T) {
		configFileName := "config.json"
		configYaml := `{
  "authentication": {
    "oidc": {
      "enabled": true,
      "issuer": "http://localhost:9090/auth/realms/weaviate",
      "username_claim": "preferred_username",
      "groups_claim": "groups",
      "client_id": "demo",
      "skip_client_id_check": false,
      "scopes": ["email", "openid"],
      "certificate": "valid-certificate"
    }
  }
}
`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.OIDC.Enabled)
		assert.Equal(t, "http://localhost:9090/auth/realms/weaviate", config.Authentication.OIDC.Issuer.Get())
		assert.Equal(t, "preferred_username", config.Authentication.OIDC.UsernameClaim.Get())
		assert.Equal(t, "groups", config.Authentication.OIDC.GroupsClaim.Get())
		assert.Equal(t, "demo", config.Authentication.OIDC.ClientID.Get())
		assert.False(t, config.Authentication.OIDC.SkipClientIDCheck.Get())
		assert.ElementsMatch(t, []string{"email", "openid"}, config.Authentication.OIDC.Scopes.Get())
		assert.Equal(t, "valid-certificate", config.Authentication.OIDC.Certificate.Get())
	})

	t.Run("parse config.yaml file with admin_list and read_only_users", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authorization:
  admin_list:
    enabled: true
    users:
      - userA
    read_only_users:
      - userA@read.only
      - userB@read.only`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authorization.AdminList.Enabled)
		assert.ElementsMatch(t, []string{"userA"}, config.Authorization.AdminList.Users)
		assert.ElementsMatch(t, []string{"userA@read.only", "userB@read.only"}, config.Authorization.AdminList.ReadOnlyUsers)
	})

	t.Run("parse config.yaml file multiple keys and users", func(t *testing.T) {
		configFileName := "config.yaml"
		configYaml := `authentication:
  apikey:
    enabled: true
    allowed_keys:
      - api-key-1
      - api-key-2
      - api-key-3
    users:
      - user1@weaviate.io
      - user2@weaviate.io`

		filepath := fmt.Sprintf("%s/%s", t.TempDir(), configFileName)
		f, err := os.Create(filepath)
		require.Nil(t, err)
		defer f.Close()
		_, err2 := f.WriteString(configYaml)
		require.Nil(t, err2)

		file, err := os.ReadFile(filepath)
		require.Nil(t, err)
		weaviateConfig := &WeaviateConfig{}
		config, err := weaviateConfig.parseConfigFile(file, configFileName)
		require.Nil(t, err)

		assert.True(t, config.Authentication.APIKey.Enabled)
		assert.ElementsMatch(t, []string{"api-key-1", "api-key-2", "api-key-3"}, config.Authentication.APIKey.AllowedKeys)
		assert.ElementsMatch(t, []string{"user1@weaviate.io", "user2@weaviate.io"}, config.Authentication.APIKey.Users)
	})
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected bool
	}{
		{
			name: "invalid combination of rbac and anon access",
			config: &Config{
				Authentication: Authentication{AnonymousAccess: AnonymousAccess{Enabled: true}},
				Authorization:  Authorization{Rbac: rbacconf.Config{Enabled: true}},
			},
			expected: true,
		},
		{
			name: "valid combination of anon access and no authorization",
			config: &Config{
				Authentication: Authentication{AnonymousAccess: AnonymousAccess{Enabled: true}},
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.Validate()
			if test.expected {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestConfigValidation_OIDCNamespaceClaims covers the cross-field invariant
// for AUTHENTICATION_OIDC_NAMESPACE_CLAIM /
// AUTHENTICATION_OIDC_GLOBAL_PRINCIPAL_CLAIM: required when NS+OIDC are
// both on, forbidden when NS=off, and unconstrained when OIDC is off.
func TestConfigValidation_OIDCNamespaceClaims(t *testing.T) {
	type wantErr struct {
		want   bool
		substr string
	}

	tests := []struct {
		name              string
		namespacesEnabled bool
		oidcEnabled       bool
		nsClaim           string
		globalClaim       string
		want              wantErr
	}{
		{
			name:              "NS+OIDC on, both claims set — pass",
			namespacesEnabled: true,
			oidcEnabled:       true,
			nsClaim:           "weaviate_namespace",
			globalClaim:       "weaviate_global_principal",
			want:              wantErr{},
		},
		{
			name:              "NS+OIDC on, namespace claim missing — fail",
			namespacesEnabled: true,
			oidcEnabled:       true,
			nsClaim:           "",
			globalClaim:       "weaviate_global_principal",
			want:              wantErr{want: true, substr: "are required when NAMESPACES_ENABLED=true"},
		},
		{
			name:              "NS+OIDC on, global claim missing — fail",
			namespacesEnabled: true,
			oidcEnabled:       true,
			nsClaim:           "weaviate_namespace",
			globalClaim:       "",
			want:              wantErr{want: true, substr: "are required when NAMESPACES_ENABLED=true"},
		},
		{
			name:              "NS off, namespace claim set — fail",
			namespacesEnabled: false,
			oidcEnabled:       true,
			nsClaim:           "weaviate_namespace",
			globalClaim:       "",
			want:              wantErr{want: true, substr: "must not be set when NAMESPACES_ENABLED=false"},
		},
		{
			name:              "NS off, global claim set — fail",
			namespacesEnabled: false,
			oidcEnabled:       true,
			nsClaim:           "",
			globalClaim:       "weaviate_global_principal",
			want:              wantErr{want: true, substr: "must not be set when NAMESPACES_ENABLED=false"},
		},
		{
			name:              "NS off, both claims unset — pass",
			namespacesEnabled: false,
			oidcEnabled:       true,
			nsClaim:           "",
			globalClaim:       "",
			want:              wantErr{},
		},
		{
			name:              "NS on, OIDC off — pass even if claims missing",
			namespacesEnabled: true,
			oidcEnabled:       false,
			nsClaim:           "",
			globalClaim:       "",
			want:              wantErr{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				Authentication: Authentication{
					APIKey: StaticAPIKey{Enabled: true, Users: []string{"u"}, AllowedKeys: []string{"k"}},
					OIDC: OIDC{
						Enabled:              tc.oidcEnabled,
						NamespaceClaim:       runtime.NewDynamicValue(tc.nsClaim),
						GlobalPrincipalClaim: runtime.NewDynamicValue(tc.globalClaim),
					},
				},
				// RBAC is required whenever NS is on; enable in lockstep.
				Authorization:  Authorization{Rbac: rbacconf.Config{Enabled: tc.namespacesEnabled}},
				DisableGraphQL: runtime.NewDynamicValue(true),
				Namespaces:     Namespaces{Enabled: tc.namespacesEnabled},
			}
			err := c.Validate()
			if tc.want.want {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.substr)
			} else if err != nil {
				assert.NotContains(t, err.Error(), "AUTHENTICATION_OIDC_NAMESPACE_CLAIM")
				assert.NotContains(t, err.Error(), "AUTHENTICATION_OIDC_GLOBAL_PRINCIPAL_CLAIM")
			}
		})
	}
}

// TestConfigValidation_Namespaces covers the two cross-field requirements that
// NAMESPACES_ENABLED=true mandates: DISABLE_GRAPHQL=true and RBAC enabled. We
// assert on the error substring to distinguish these from downstream validation
// noise. The graphql check runs before the RBAC check, so each row isolates a
// single expected failure.
func TestConfigValidation_Namespaces(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		disableGraphQL    bool
		rbacEnabled       bool
		// wantErrSubstr is empty when no namespace cross-field error is expected.
		wantErrSubstr string
	}{
		{
			name:              "namespaces disabled — no namespace error",
			namespacesEnabled: false,
			disableGraphQL:    true,
			rbacEnabled:       false,
			wantErrSubstr:     "",
		},
		{
			name:              "namespaces enabled without RBAC — requires RBAC error",
			namespacesEnabled: true,
			disableGraphQL:    true,
			rbacEnabled:       false,
			wantErrSubstr:     "NAMESPACES_ENABLED=true requires RBAC to be enabled",
		},
		{
			name:              "namespaces enabled without DISABLE_GRAPHQL — requires DISABLE_GRAPHQL error",
			namespacesEnabled: true,
			disableGraphQL:    false,
			rbacEnabled:       true,
			wantErrSubstr:     "NAMESPACES_ENABLED=true requires DISABLE_GRAPHQL=true",
		},
		{
			name:              "namespaces enabled with DISABLE_GRAPHQL and RBAC — no namespace error",
			namespacesEnabled: true,
			disableGraphQL:    true,
			rbacEnabled:       true,
			wantErrSubstr:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &Config{
				Authentication: Authentication{APIKey: StaticAPIKey{Enabled: true, Users: []string{"u"}, AllowedKeys: []string{"k"}}},
				Authorization:  Authorization{Rbac: rbacconf.Config{Enabled: tc.rbacEnabled}},
				DisableGraphQL: runtime.NewDynamicValue(tc.disableGraphQL),
				Namespaces:     Namespaces{Enabled: tc.namespacesEnabled},
			}
			err := c.Validate()
			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSubstr)
			} else if err != nil {
				// Validate may still fail on unrelated fields (e.g. Persistence);
				// only assert it does NOT fail on either namespace cross-field check.
				assert.NotContains(t, err.Error(), "NAMESPACES_ENABLED=true requires DISABLE_GRAPHQL=true")
				assert.NotContains(t, err.Error(), "NAMESPACES_ENABLED=true requires RBAC to be enabled")
			}
		})
	}
}
