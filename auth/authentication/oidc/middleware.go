package oidc

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc"
	"github.com/creativesoftwarefdn/weaviate/config"
	"github.com/creativesoftwarefdn/weaviate/models"
	errors "github.com/go-openapi/errors"
)

// Client handles the OIDC setup at startup and provides a middleware to tbe
// used with the goswagger API
type Client struct {
	config   config.OIDC
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// New OIDC Client: It tries to retrieve the JWKs at startup (or fails), it
// provides a middleware which can be used at runtime with a go-swagger style
// API
func New(cfg config.Environment) (*Client, error) {
	client := &Client{
		config: cfg.Authentication.OIDC,
	}

	if !client.config.Enabled {
		// if oidc is not enabled, we are done, no need to setup an actual client.
		// The "disabled" client is however still valuable to deny any requests
		// coming in with an OAuth token set.
		return client, nil
	}

	if err := client.init(); err != nil {
		return nil, fmt.Errorf("oidc init: %v", err)
	}

	return client, nil
}

func (c *Client) init() error {
	provider, err := oidc.NewProvider(context.Background(), c.config.Issuer)
	if err != nil {
		return fmt.Errorf("could not setup provider: %v", err)
	}
	c.provider = provider

	verifier := provider.Verifier(&oidc.Config{
		ClientID: c.config.ClientID,
	})
	c.verifier = verifier

	return nil
}

// ValidateAndExtract can be used as a middleware for go-swagger
func (c *Client) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if !c.config.Enabled {
		return nil, errors.New(401, "oidc auth is not configured, please try another auth scheme or set up weaviate with OIDC configured")
	}

	parsed, err := c.verifier.Verify(context.Background(), token)
	if err != nil {
		return nil, errors.New(401, err.Error())
	}

	return &models.Principal{
		Username: parsed.Subject,
	}, nil
}
