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

package composer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_TokenAuthComposer(t *testing.T) {
	type test struct {
		name         string
		token        string
		config       config.Authentication
		oidc         TokenFunc
		apiKey       TokenFunc
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
		{
			name: "both an enabled, with an 'obvious' api key",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: true,
				},
				APIKey: config.APIKey{
					Enabled: true,
				},
			},
			token: "does not matter",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("it's a pretty key, but not good enough")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			expectErr:    true,
			expectErrMsg: "it's a pretty key, but not good enough",
		},
		{
			name: "both an enabled, empty token",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: true,
				},
				APIKey: config.APIKey{
					Enabled: true,
				},
			},
			token: "",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("really? an empty one?")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			expectErr:    true,
			expectErrMsg: "empty one",
		},
		{
			name: "both an enabled, jwt token",
			config: config.Authentication{
				OIDC: config.OIDC{
					Enabled: true,
				},
				APIKey: config.APIKey{
					Enabled: true,
				},
			},
			token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
			apiKey: func(t string, s []string) (*models.Principal, error) {
				panic("i should never be called")
			},
			oidc: func(t string, s []string) (*models.Principal, error) {
				return nil, fmt.Errorf("john doe ... that sounds like a fake name!")
			},
			expectErr:    true,
			expectErrMsg: "john doe",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := New(
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
	v TokenFunc
}

func (v fakeValidator) ValidateAndExtract(t string, s []string) (*models.Principal, error) {
	return v.v(t, s)
}
