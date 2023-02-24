package apikey

import (
	"fmt"

	errors "github.com/go-openapi/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

type Client struct {
	config config.APIKey
}

func New(cfg config.Config) (*Client, error) {
	c := &Client{
		config: cfg.Authentication.APIKey,
	}

	if err := c.validateConfig(); err != nil {
		return nil, fmt.Errorf("invalid apikey config: %w", err)
	}

	return c, nil
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

	return nil
}

func (c *Client) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if !c.config.Enabled {
		return nil, errors.New(401, "apikey auth is not configured, please try another auth scheme or set up weaviate with apikey configured")
	}

	if !c.isTokenAllowed(token) {
		return nil, errors.New(401, "invalid api key, please provide a valid api key")
	}

	return &models.Principal{
		Username: c.config.Username,
	}, nil
}

func (c *Client) isTokenAllowed(token string) bool {
	for _, allowed := range c.config.AllowedKeys {
		if token == allowed {
			return true
		}
	}

	return false
}
