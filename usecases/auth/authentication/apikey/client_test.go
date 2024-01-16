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

package apikey

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_APIKeyClient(t *testing.T) {
	type test struct {
		name               string
		config             config.APIKey
		expectConfigErr    bool
		expectConfigErrMsg string
		validate           func(t *testing.T, c *Client)
	}

	tests := []test{
		{
			name: "not enabled",
			config: config.APIKey{
				Enabled: false,
			},
			expectConfigErr: false,
		},
		{
			name: "key, but no user",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key"},
				Users:       []string{},
			},
			expectConfigErr:    true,
			expectConfigErrMsg: "need at least one user",
		},
		{
			name: "zero length key",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{""},
				Users:       []string{"gooduser"},
			},
			expectConfigErr:    true,
			expectConfigErrMsg: "keys cannot have length 0",
		},
		{
			name: "user, but no key",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{},
				Users:       []string{"johnnyBeAllowed"},
			},
			expectConfigErr:    true,
			expectConfigErrMsg: "need at least one valid allowed key",
		},
		{
			name: "zero length user",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key"},
				Users:       []string{""},
			},
			expectConfigErr:    true,
			expectConfigErrMsg: "users cannot have length 0",
		},
		{
			name: "one user, one key",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key"},
				Users:       []string{"mrRoboto"},
			},
			expectConfigErr: false,
			validate: func(t *testing.T, c *Client) {
				p, err := c.ValidateAndExtract("secret-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "mrRoboto", p.Username)

				_, err = c.ValidateAndExtract("", nil)
				require.NotNil(t, err)
				_, err = c.ValidateAndExtract("other-key", nil)
				require.NotNil(t, err)
			},
		},
		{
			// this is allowed, this means that all keys point to the same user for
			// authZ purposes
			name: "one user, multiple keys",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key", "another-secret-key", "third-key"},
				Users:       []string{"jane"},
			},
			expectConfigErr: false,
			validate: func(t *testing.T, c *Client) {
				p, err := c.ValidateAndExtract("secret-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jane", p.Username)

				p, err = c.ValidateAndExtract("another-secret-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jane", p.Username)

				p, err = c.ValidateAndExtract("third-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jane", p.Username)

				_, err = c.ValidateAndExtract("", nil)
				require.NotNil(t, err)
				_, err = c.ValidateAndExtract("other-key", nil)
				require.NotNil(t, err)
			},
		},
		{
			// this is allowed, this means that each key at pos i points to user at
			// pos i for authZ purposes
			name: "multiple user, multiple keys",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key", "another-secret-key", "third-key"},
				Users:       []string{"jane", "jessica", "jennifer"},
			},
			expectConfigErr: false,
			validate: func(t *testing.T, c *Client) {
				p, err := c.ValidateAndExtract("secret-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jane", p.Username)

				p, err = c.ValidateAndExtract("another-secret-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jessica", p.Username)

				p, err = c.ValidateAndExtract("third-key", nil)
				require.Nil(t, err)
				assert.Equal(t, "jennifer", p.Username)

				_, err = c.ValidateAndExtract("", nil)
				require.NotNil(t, err)
				_, err = c.ValidateAndExtract("other-key", nil)
				require.NotNil(t, err)
			},
		},
		{
			// this is invalid, the keys cannot be mapped to the users
			name: "2 users, 3 keys",
			config: config.APIKey{
				Enabled:     true,
				AllowedKeys: []string{"secret-key", "another-secret-key", "third-key"},
				Users:       []string{"jane", "jessica"},
			},
			expectConfigErr:    true,
			expectConfigErrMsg: "length of users and keys must match, alternatively provide single user for all keys",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := New(config.Config{
				Authentication: config.Authentication{
					APIKey: test.config,
				},
			})

			if test.expectConfigErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expectConfigErrMsg)
				return
			}

			if test.validate != nil {
				test.validate(t, c)
			}
		})
	}
}
