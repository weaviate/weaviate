//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
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
	"os"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	errors "github.com/go-openapi/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// Client handles the OIDC setup at startup and provides a middleware to be
// used with the goswagger API
type Client struct {
	Config            config.OIDC
	verifier          *oidc.IDTokenVerifier
	logger            logrus.FieldLogger
	nsExister         namespaces.Exister
	namespacesEnabled bool
	// rbac is consulted on namespace-enabled clusters to reject tokens that
	// would produce a namespaced principal carrying the root role (root is
	// cluster-global).
	rbac rbacconf.Config
}

// New OIDC Client: It tries to retrieve the JWKs at startup (or fails), it
// provides a middleware which can be used at runtime with a go-swagger style
// API.
//
// nsExister is consulted only on namespace-enabled clusters to validate the
// namespace claim. namespacesEnabled is the cluster-level flag passed in
// from the caller.
func New(cfg config.Config, nsExister namespaces.Exister, namespacesEnabled bool, logger logrus.FieldLogger) (*Client, error) {
	client := &Client{
		Config:            cfg.Authentication.OIDC,
		logger:            logger.WithField("component", "oidc"),
		nsExister:         nsExister,
		namespacesEnabled: namespacesEnabled,
		rbac:              cfg.Authorization.Rbac,
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
	c.logger.WithField("action", "oidc_init").Info("validated OIDC configuration")

	ctx := context.Background()
	if c.Config.Certificate.Get() != "" || c.Config.SkipTLSVerify.Get() {
		client, err := c.buildHTTPClient()
		if err != nil {
			return fmt.Errorf("could not setup OIDC HTTP client: %w", err)
		}
		ctx = oidc.ClientContext(ctx, client)
	}

	if c.Config.JWKSUrl.Get() != "" {
		keySet := oidc.NewRemoteKeySet(ctx, c.Config.JWKSUrl.Get())
		verifier := oidc.NewVerifier(c.Config.Issuer.Get(), keySet, &oidc.Config{
			ClientID:          c.Config.ClientID.Get(),
			SkipClientIDCheck: c.Config.SkipClientIDCheck.Get(),
		})
		c.verifier = verifier
		c.logger.WithField("action", "oidc_init").WithField("jwks_url", c.Config.JWKSUrl.Get()).Info("configured OIDC verifier")
	} else {
		provider, err := oidc.NewProvider(ctx, c.Config.Issuer.Get())
		if err != nil {
			return fmt.Errorf("could not setup provider: %w", err)
		}
		c.logger.WithField("action", "oidc_init").Info("configured OIDC provider")

		// oauth2

		verifier := provider.Verifier(&oidc.Config{
			ClientID:          c.Config.ClientID.Get(),
			SkipClientIDCheck: c.Config.SkipClientIDCheck.Get(),
		})
		c.verifier = verifier
		c.logger.WithField("action", "oidc_init").Info("configured OIDC verifier")
	}

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

	if c.Config.Certificate.Get() != "" && c.Config.SkipTLSVerify.Get() {
		msgs = append(msgs, "custom OIDC certificate and insecure_skip_tls_verify are mutually exclusive: "+
			"remove the certificate or disable insecure_skip_tls_verify")
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

	namespace, isGlobal, err := c.classifyPrincipal(claims, username)
	if err != nil {
		return nil, err
	}

	qualifiedUsername := namespacing.QualifiedName(namespace, username)

	if err := c.rejectNamespacedRoot(namespace, qualifiedUsername, groups); err != nil {
		return nil, err
	}

	return &models.Principal{
		Username:         qualifiedUsername,
		Groups:           groups,
		UserType:         models.UserTypeInputOidc,
		Namespace:        namespace,
		IsGlobalOperator: isGlobal,
	}, nil
}

// rejectNamespacedRoot returns 401 when the token would produce a
// namespaced principal that also carries the root role via RootUsers or
// RootGroups. Returns nil otherwise.
func (c *Client) rejectNamespacedRoot(namespace, qualifiedUsername string, groups []string) error {
	if namespace == "" {
		return nil
	}
	if !c.rbac.IsRoot(qualifiedUsername, groups) {
		return nil
	}
	return errors.New(401, "unauthorized: namespaced OIDC principal cannot be granted the root role; remove the namespace claim or remove the principal from RBAC root configuration")
}

// classifyPrincipal resolves the namespace and global-operator flag for an
// OIDC token's claims. On namespace-disabled clusters it short-circuits to
// the legacy "global, no namespace" shape — startup validation guarantees
// the claim env vars are empty in that case, so the classifier has nothing
// to inspect.
//
// On namespace-enabled clusters the rule matrix is:
//
//	namespace claim    | global claim     | result
//	-------------------+------------------+-----------------------------
//	non-empty          | absent OR false  | namespaced (validate exists)
//	absent             | true             | global operator
//	non-empty          | true             | reject (both)
//	absent             | absent OR false  | reject (neither)
//
// "absent" covers both missing keys and empty-string values for the
// namespace claim — an empty namespace name carries no information.
//
// Type-mismatched claims (namespace not a string, global-principal not a
// bool) return 401 — a malformed claim is a token the server cannot
// interpret, which is an authentication failure from the caller's
// perspective.
//
// On namespace-enabled clusters, the resolved username must not contain
// ':' (the namespace separator); reject with 401.
func (c *Client) classifyPrincipal(claims map[string]interface{}, username string) (namespace string, isGlobal bool, err error) {
	if !c.namespacesEnabled {
		return "", false, nil
	}

	if strings.Contains(username, schema.NamespaceSeparator) {
		return "", false, errors.New(401, "unauthorized: OIDC username '%s' must not contain ':' on a namespace-enabled cluster", username)
	}

	nsClaimKey := ""
	if c.Config.NamespaceClaim != nil {
		nsClaimKey = c.Config.NamespaceClaim.Get()
	}
	globalClaimKey := ""
	if c.Config.GlobalPrincipalClaim != nil {
		globalClaimKey = c.Config.GlobalPrincipalClaim.Get()
	}

	nsValue := ""
	if nsClaimKey != "" {
		if raw, ok := claims[nsClaimKey]; ok {
			s, isStr := raw.(string)
			if !isStr {
				return "", false, errors.New(401, "unauthorized: namespace claim '%s' is not a string", nsClaimKey)
			}
			nsValue = s
		}
	}

	globalSet := false
	globalValue := false
	if globalClaimKey != "" {
		if raw, ok := claims[globalClaimKey]; ok {
			b, isBool := raw.(bool)
			if !isBool {
				return "", false, errors.New(401, "unauthorized: global-principal claim '%s' is not a bool", globalClaimKey)
			}
			globalSet = true
			globalValue = b
		}
	}

	switch {
	case nsValue != "" && globalSet && globalValue:
		return "", false, errors.New(401, "unauthorized: token must not carry both a namespace claim and a global-principal claim set to true")
	case nsValue != "":
		// Error message is intentionally vague to avoid leaking namespace
		// existence to unauthenticated clients.
		if !c.nsExister.IsActive(nsValue) {
			return "", false, errors.New(401, "unauthorized: namespace '%s' does not exist or is being deleted", nsValue)
		}
		return nsValue, false, nil
	case globalSet && globalValue:
		return "", true, nil
	default:
		return "", false, errors.New(401, "unauthorized: token must carry either a namespace claim or a global-principal claim set to true")
	}
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
		groupAsString, ok := groupsUntyped.(string)
		if ok {
			return []string{groupAsString}
		}
		return groups
	}

	for _, untyped := range groupsSlice {
		if group, ok := untyped.(string); ok {
			groups = append(groups, group)
		}
	}

	return groups
}

// buildHTTPClient creates an HTTP client with custom TLS settings derived from
// the OIDC config. It loads a custom certificate pool when a certificate is
// configured, and disables TLS verification when SkipTLSVerify is set.
func (c *Client) buildHTTPClient() (*http.Client, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if c.Config.Certificate.Get() != "" {
		certPool, err := c.loadCertPool()
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = certPool
		c.logger.WithField("action", "oidc_init").Info("configured OIDC client with custom certificate")
	}

	if c.Config.SkipTLSVerify.Get() {
		tlsCfg.InsecureSkipVerify = true // #nosec G402 -- opt-in via AUTHENTICATION_OIDC_INSECURE_SKIP_TLS_VERIFY
		c.logger.WithField("action", "oidc_init").Warn("TLS verification disabled for OIDC connections — do not use in production")
	}

	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}, nil
}

// loadCertPool fetches the certificate from the configured source (HTTP URL,
// S3 URI, or inline PEM string) and returns a certificate pool containing it.
// Note: HTTP URL fetches use the default http.Client, so the certificate URL
// must be reachable without custom TLS settings. Certificate and SkipTLSVerify
// are mutually exclusive, so this function is only called when SkipTLSVerify
// is false.
func (c *Client) loadCertPool() (*x509.CertPool, error) {
	var certificate, certificateSource string
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
		certificateSource = c.Config.Certificate.Get()
	} else if strings.HasPrefix(c.Config.Certificate.Get(), "s3://") {
		parts := strings.TrimPrefix(c.Config.Certificate.Get(), "s3://")
		segments := strings.SplitN(parts, "/", 2)
		if len(segments) != 2 {
			return nil, fmt.Errorf("invalid S3 URI, must contain bucket and key: %s", c.Config.Certificate.Get())
		}
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
		creds := credentials.NewIAM("")
		// check if we are able to get the credentials using AWS IAM
		if _, err := creds.GetWithContext(nil); err != nil {
			// if IAM doesn't work, check environment settings for creds, set anonymous access if none found
			creds = credentials.NewEnvAWS()
		}
		bucketName, objectKey := segments[0], segments[1]
		minioClient, err := minio.New("s3.amazonaws.com", &minio.Options{
			Creds:  creds,
			Secure: true,
			Region: region,
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
		certificateSource = c.Config.Certificate.Get()
	} else {
		certificate = c.Config.Certificate.Get()
		certificateSource = "environment variable"
	}

	certBlock, _ := pem.Decode([]byte(certificate))
	if certBlock == nil || len(certBlock.Bytes) == 0 {
		return nil, fmt.Errorf("failed to decode certificate")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	c.logger.WithField("action", "oidc_init").WithField("source", certificateSource).Info("custom certificate is valid")

	certPool := x509.NewCertPool()
	certPool.AddCert(cert)
	return certPool, nil
}
