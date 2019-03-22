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

	claims, err := c.extractClaims(parsed)
	if err != nil {
		return nil, errors.New(500, fmt.Sprintf("oidc: %v", err))
	}

	username, err := c.extractUsername(claims)
	if err != nil {
		return nil, errors.New(500, fmt.Sprintf("oidc: %v", err))
	}

	groups := c.extractGroups(claims)

	return &models.Principal{
		Username: username,
		Groups:   groups,
	}, nil
}

func (c *Client) extractClaims(token *oidc.IDToken) (map[string]interface{}, error) {
	var claims map[string]interface{}
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("could not extract claims from token: %v", err)
	}

	return claims, nil
}

func (c *Client) extractUsername(claims map[string]interface{}) (string, error) {
	usernameUntyped, ok := claims[c.config.UsernameClaim]
	if !ok {
		return "", fmt.Errorf("token doesn't contain required claim '%s'", c.config.UsernameClaim)
	}

	username, ok := usernameUntyped.(string)
	if !ok {
		return "", fmt.Errorf("claim '%s' is not a string, but %T", c.config.UsernameClaim, usernameUntyped)
	}

	return username, nil
}

// extractGroups never errors, if groups cant be parsed an empty set of groups
// is returned. This is because groups are not a required standard in the OIDC
// spec, so we can't error if an OIDC provider does not support them.
func (c *Client) extractGroups(claims map[string]interface{}) []string {
	var groups []string

	groupsUntyped, ok := claims[c.config.GroupsClaim]
	if !ok {
		return groups
	}

	groupsSlice, ok := groupsUntyped.([]interface{})
	if !ok {
		return groups
	}

	for _, untyped := range groupsSlice {
		if group, ok := untyped.(string); ok {
			groups = append(groups, group)
		}
	}

	return groups
}
