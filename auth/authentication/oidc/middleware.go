package oidc

import (
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/models"
	errors "github.com/go-openapi/errors"
)

// Client handles the OIDC setup at startup and provides a middleware to tbe
// used with the goswagger API
type Client struct {
	config config.OIDC
}

// New OIDC Client: It tries to retrieve the JWKs at startup (or fails), it
// provides a middleware which can be used at runtime with a go-swagger style
// API
func New(cfg config.Environment) (*Client, error) {
	client := Client{
		config: cfg.Authentication.OIDC,
	}

	return &client, nil
}

// ValidateAndExtract can be used as a middleware for go-swagger
func (c *Client) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if !c.config.Enabled {
		return nil, errors.New(401, "oidc auth is not configured, please try another auth scheme or set up weaviate with OIDC configured")
	}

	return nil, errors.New(401, "foo")
}
