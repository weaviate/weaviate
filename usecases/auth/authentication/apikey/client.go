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
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	errors "github.com/go-openapi/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type Client struct {
	config     config.APIKey
	keystorage [][sha256.Size]byte
}

func New(cfg config.Config) (*Client, error) {
	c := &Client{
		config: cfg.Authentication.APIKey,
	}

	if err := c.validateConfig(); err != nil {
		return nil, fmt.Errorf("invalid apikey config: %w", err)
	}

	c.parseKeys()

	return c, nil
}

func (c *Client) parseKeys() {
	c.keystorage = make([][sha256.Size]byte, len(c.config.AllowedKeys))
	for i, rawKey := range c.config.AllowedKeys {
		c.keystorage[i] = sha256.Sum256([]byte(rawKey))
	}
}

func (c *Client) validateConfig() error {
	if !c.config.Enabled {
		// don't validate if this scheme isn't used
		return nil
	}

	if len(c.config.AllowedKeys) < 1 {
		return fmt.Errorf("need at least one valid allowed key")
	}

	for _, key := range c.config.AllowedKeys {
		if len(key) == 0 {
			return fmt.Errorf("keys cannot have length 0")
		}
	}

	if len(c.config.Users) < 1 {
		return fmt.Errorf("need at least one user")
	}

	for _, key := range c.config.Users {
		if len(key) == 0 {
			return fmt.Errorf("users cannot have length 0")
		}
	}

	if len(c.config.Users) > 1 && len(c.config.Users) != len(c.config.AllowedKeys) {
		return fmt.Errorf("length of users and keys must match, alternatively provide single user for all keys")
	}

	return nil
}

func (c *Client) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if !c.config.Enabled {
		return nil, errors.New(401, "apikey auth is not configured, please try another auth scheme or set up weaviate with apikey configured")
	}

	tokenPos, ok := c.isTokenAllowed(token)
	if !ok {
		return nil, errors.New(401, "invalid api key, please provide a valid api key")
	}

	return &models.Principal{
		Username: c.getUser(tokenPos),
	}, nil
}

func (c *Client) isTokenAllowed(token string) (int, bool) {
	tokenHash := sha256.Sum256([]byte(token))

	for i, allowed := range c.keystorage {
		if subtle.ConstantTimeCompare(tokenHash[:], allowed[:]) == 1 {
			return i, true
		}
	}

	return -1, false
}

func (c *Client) getUser(pos int) string {
	// passed validation guarantees that one of those options will work
	if pos >= len(c.config.Users) {
		return c.config.Users[0]
	}

	return c.config.Users[pos]
}
