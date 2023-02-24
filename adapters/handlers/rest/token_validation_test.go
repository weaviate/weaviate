package rest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_OpenAPITokenValidator(t *testing.T) {
	type test struct {
		name         string
		token        string
		config       config.Authentication
		oidc         openAPITokenFunc
		apiKey       openAPITokenFunc
		expectErr    bool
		expectErrMsg string
	}

	tests := []test{
		{
			name: "everything disabled - pass to oidc provider (backward compat)",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: false,
				},
				APIKey: config.APIKey{
					Enabled: false,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				return nil, nil
			},
			expectErr: false,
		},
		{
			name: "everything disabled - pass to oidc provider fail",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: false,
				},
				APIKey: config.APIKey{
					Enabled: false,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("oidc says nope!")
			},
			expectErr:    true,
			expectErrMsg: "oidc says nope!",
		},
		{
			name: "only oidc enabled, returns success",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: true,
				},
				APIKey: config.APIKey{
					Enabled: false,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				return nil, nil
			},
			expectErr: false,
		},
		{
			name: "only oidc enabled, returns no success",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: true,
				},
				APIKey: config.APIKey{
					Enabled: false,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("thou shalt not pass")
			},
			expectErr:    true,
			expectErrMsg: "thou shalt not pass",
		},
		{
			name: "only apiKey enabled, returns success",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: false,
				},
				APIKey: config.APIKey{
					Enabled: true,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				return nil, nil
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			expectErr: false,
		},
		{
			name: "only apiKey enabled, returns no success",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: false,
				},
				APIKey: config.APIKey{
					Enabled: true,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("you think I let anyone through?")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			expectErr:    true,
			expectErrMsg: "you think I let anyone through?",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := NewOpenAPITokenValidator(
				test.config,
				fakeValidator{v: test.apiKey},
				fakeValidator{v: test.oidc},
			)
			_, err := v(test.token, nil)
			if test.expectErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expectErrMsg)
				return
			}

			require.Nil(t, err)
		})
	}
}

type fakeValidator struct {
	v openAPITokenFunc
}

func (v fakeValidator) ValidateAndExtract(t string, s []string) (*models.Principal, error) {
	return v.v(t, s)
}
