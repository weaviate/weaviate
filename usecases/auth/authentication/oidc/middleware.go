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

package oidc

import (
	"bytes"
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"
)

// Client handles the OIDC setup at startup and provides a middleware to be
// used with the goswagger API
type Client struct {
	Config   config.OIDC
	provider *oidc.Provider
	verifier *oidc.IDTokenVerifier
}

// New OIDC Client: It tries to retrieve the JWKs at startup (or fails), it
// provides a middleware which can be used at runtime with a go-swagger style
// API
func New(cfg config.Config) (*Client, error) {
	client := &Client{
		Config: cfg.Authentication.OIDC,
	}

	if !client.Config.Enabled {
		// if oidc is not enabled, we are done, no need to setup an actual client.
		// The "disabled" client is however still valuable to deny any requests
		// coming in with an OAuth token set.
		return client, nil
	}

	if err := client.Init(); err != nil {
		return nil, fmt.Errorf("oidc init: %w", err)
	}

	return client, nil
}

func (c *Client) Init() error {
	if err := c.validateConfig(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	ctx := context.Background()
	if c.Config.Certificate.Get() != "" {
		client, err := c.useCertificate()
		if err != nil {
			return fmt.Errorf("could not setup client with custom certificate: %w", err)
		}
		ctx = oidc.ClientContext(ctx, client)
	}

	provider, err := oidc.NewProvider(ctx, c.Config.Issuer.Get())
	if err != nil {
		return fmt.Errorf("could not setup provider: %w", err)
	}
	c.provider = provider

	// oauth2

	verifier := provider.Verifier(&oidc.Config{
		ClientID:          c.Config.ClientID.Get(),
		SkipClientIDCheck: c.Config.SkipClientIDCheck.Get(),
	})
	c.verifier = verifier

	return nil
}

func (c *Client) validateConfig() error {
	var msgs []string

	if c.Config.Issuer.Get() == "" {
		msgs = append(msgs, "missing required field 'issuer'")
	}

	if c.Config.UsernameClaim.Get() == "" {
		msgs = append(msgs, "missing required field 'username_claim'")
	}

	if !c.Config.SkipClientIDCheck.Get() && c.Config.ClientID.Get() == "" {
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
	if !c.Config.Enabled {
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
	usernameUntyped, ok := claims[c.Config.UsernameClaim.Get()]
	if !ok {
		return "", fmt.Errorf("token doesn't contain required claim '%s'", c.Config.UsernameClaim.Get())
	}

	username, ok := usernameUntyped.(string)
	if !ok {
		return "", fmt.Errorf("claim '%s' is not a string, but %T", c.Config.UsernameClaim.Get(), usernameUntyped)
	}

	return username, nil
}

// extractGroups never errors, if groups can't be parsed an empty set of groups
// is returned. This is because groups are not a required standard in the OIDC
// spec, so we can't error if an OIDC provider does not support them.
func (c *Client) extractGroups(claims map[string]interface{}) []string {
	var groups []string

	groupsUntyped, ok := claims[c.Config.GroupsClaim.Get()]
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
	if strings.HasPrefix(c.Config.Certificate.Get(), "http") {
		resp, err := http.Get(c.Config.Certificate.Get())
		if err != nil {
			return nil, fmt.Errorf("failed to get certificate from %s: %w", c.Config.Certificate.Get(), err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to download certificate from %s: http status: %v", c.Config.Certificate.Get(), resp.StatusCode)
		}
		certBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read certificate from %s: %w", c.Config.Certificate.Get(), err)
		}
		certificate = string(certBytes)
	} else if strings.HasPrefix(c.Config.Certificate.Get(), "s3://") {
		parts := strings.TrimPrefix(c.Config.Certificate.Get(), "s3://")
		segments := strings.SplitN(parts, "/", 2)
		if len(segments) != 2 {
			return nil, fmt.Errorf("invalid S3 URI, must contain bucket and key: %s", c.Config.Certificate.Get())
		}
		bucketName, objectKey := segments[0], segments[1]
		minioClient, err := minio.New("s3.amazonaws.com", &minio.Options{
			Creds:  credentials.NewEnvAWS(),
			Secure: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}
		object, err := minioClient.GetObject(context.Background(), bucketName, objectKey, minio.GetObjectOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get certificate from: %s: %w", c.Config.Certificate.Get(), err)
		}
		defer object.Close()
		var content bytes.Buffer
		if _, err := io.Copy(&content, object); err != nil {
			return nil, fmt.Errorf("failed to read certificate from %s: %w", c.Config.Certificate.Get(), err)
		}
		certificate = content.String()
	} else {
		certificate = c.Config.Certificate.Get()
	}

	certBlock, _ := pem.Decode([]byte(certificate))
	if certBlock == nil || len(certBlock.Bytes) == 0 {
		return nil, fmt.Errorf("failed to decode certificate")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
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
