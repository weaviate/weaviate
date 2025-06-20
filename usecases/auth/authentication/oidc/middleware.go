//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package oidc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	errors "github.com/go-openapi/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

// Client handles the OIDC setup at startup and provides a middleware to be
// used with the goswagger API
type Client struct {
	config   config.OIDC
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// New OIDC Client: It tries to retrieve the JWKs at startup (or fails), it
// provides a middleware which can be used at runtime with a go-swagger style
// API
func New(cfg config.Config) (*Client, error) {
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
		return nil, fmt.Errorf("oidc init: %w", err)
	}

	return client, nil
}

func (c *Client) init() error {
	if err := c.validateConfig(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	ctx := context.Background()
	if c.config.Certificate != "" {
		client, err := c.useCertificate()
		if err != nil {
			return fmt.Errorf("could not setup client with custom certificate: %w", err)
		}
		ctx = oidc.ClientContext(ctx, client)
	}

	provider, err := oidc.NewProvider(ctx, c.config.Issuer)
	if err != nil {
		return fmt.Errorf("could not setup provider: %w", err)
	}
	c.provider = provider

	// oauth2

	verifier := provider.Verifier(&oidc.Config{
		ClientID:          c.config.ClientID,
		SkipClientIDCheck: c.config.SkipClientIDCheck,
	})
	c.verifier = verifier

	return nil
}

func (c *Client) validateConfig() error {
	var msgs []string

	if c.config.Issuer == "" {
		msgs = append(msgs, "missing required field 'issuer'")
	}

	if c.config.UsernameClaim == "" {
		msgs = append(msgs, "missing required field 'username_claim'")
	}

	if !c.config.SkipClientIDCheck && c.config.ClientID == "" {
		msgs = append(msgs, "missing required field 'client_id': "+
			"either set a client_id or explicitly disable the check with 'skip_client_id_check: true'")
	}

	if len(msgs) == 0 {
		return nil
	}

	return fmt.Errorf("%v", strings.Join(msgs, ", "))
}

// ValidateAndExtract can be used as a middleware for go-swagger
func (c *Client) ValidateAndExtract(token string, scopes []string) (*models.Principal, error) {
	if !c.config.Enabled {
		return nil, errors.New(401, "oidc auth is not configured, please try another auth scheme or set up weaviate with OIDC configured")
	}

	parsed, err := c.verifier.Verify(context.Background(), token)
	if err != nil {
		return nil, errors.New(401, "unauthorized: %v", err)
	}

	claims, err := c.extractClaims(parsed)
	if err != nil {
		return nil, errors.New(500, "oidc: %v", err)
	}

	username, err := c.extractUsername(claims)
	if err != nil {
		return nil, errors.New(500, "oidc: %v", err)
	}

	groups := c.extractGroups(claims)

	return &models.Principal{
		Username: username,
		Groups:   groups,
		UserType: models.UserTypeInputOidc,
	}, nil
}

func (c *Client) extractClaims(token *oidc.IDToken) (map[string]interface{}, error) {
	var claims map[string]interface{}
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("could not extract claims from token: %w", err)
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

// extractGroups never errors, if groups can't be parsed an empty set of groups
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

func (c *Client) useCertificate() (*http.Client, error) {
	var certificate string
	if strings.HasPrefix(c.config.Certificate, "http") {
		resp, err := http.Get(c.config.Certificate)
		if err != nil {
			return nil, fmt.Errorf("failed to get certificate from %s: %w", c.config.Certificate, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to download certificate from %s: http status: %v", c.config.Certificate, resp.StatusCode)
		}
		certBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read certificate from %s: %w", c.config.Certificate, err)
		}
		certificate = string(certBytes)
	} else {
		certificate = c.config.Certificate
	}

	certBlock, _ := pem.Decode([]byte(certificate))
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode certificate: %w", err)
	}

	certPool := x509.NewCertPool()
	certPool.AddCert(cert)

	// Create an HTTP client with self signed certificate
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				MinVersion: tls.VersionTLS12,
			},
		},
	}

	return client, nil
}
