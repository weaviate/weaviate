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

package awscommon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type AuthBrokerCredentials struct {
	credentials.Expiry
	endpoint          string
	identityTokenPath string
	client            *http.Client
	tokenTimeout      time.Duration
}

type AuthBrokerCredentialValue struct {
	AccessKeyID     string    `json:"access_key_id"`
	SecretAccessKey string    `json:"secret_access_key"`
	SessionToken    string    `json:"session_token"`
	Expiration      time.Time `json:"expiration"`
}

const (
	httpClientTimeout   = 5 * time.Second
	defaultTokenTimeout = 30 * time.Second
	identityFileEnvVar  = "AUTH_PROXY_IDENTITY_FILE"
	tokenTimeoutEnvVar  = "AUTH_PROXY_TOKEN_TIMEOUT"
)

var ErrRetryableAuthBroker = errors.New("retryable error from auth broker")

func NewAuthBrokerCredentials(endpoint string) (*AuthBrokerCredentials, error) {
	path := os.Getenv(identityFileEnvVar)
	if path == "" {
		return nil, fmt.Errorf("%s not set; auth broker requires identity token file", identityFileEnvVar)
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("auth broker identity file %q not readable: %w", path, err)
	}
	return &AuthBrokerCredentials{
		endpoint:          endpoint,
		identityTokenPath: path,
		client:            &http.Client{Timeout: httpClientTimeout},
		tokenTimeout:      resolveTokenTimeout(),
	}, nil
}

func resolveTokenTimeout() time.Duration {
	v, ok := os.LookupEnv(tokenTimeoutEnvVar)
	if !ok || v == "" {
		return defaultTokenTimeout
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return defaultTokenTimeout
	}
	return d
}

func (b *AuthBrokerCredentials) Retrieve() (credentials.Value, error) {
	return b.RetrieveWithCredContext(nil)
}

func (b *AuthBrokerCredentials) RetrieveWithCredContext(_ *credentials.CredContext) (credentials.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), b.tokenTimeout)
	defer cancel()

	identityToken, err := b.readIdentityToken()
	if err != nil {
		return credentials.Value{}, err
	}

	creds, err := b.fetchCredentialsWithRetry(ctx, identityToken)
	if err != nil {
		return credentials.Value{}, err
	}

	// A negative window asks Expiry to apply its default 20% safety margin,
	// so the SDK refreshes before the broker-issued session actually expires.
	b.SetExpiration(creds.Expiration, -1)

	return credentials.Value{
		AccessKeyID:     creds.AccessKeyID,
		SecretAccessKey: creds.SecretAccessKey,
		SessionToken:    creds.SessionToken,
		Expiration:      creds.Expiration,
		SignerType:      credentials.SignatureV4,
	}, nil
}

func (b *AuthBrokerCredentials) fetchCredentialsWithRetry(ctx context.Context, identityToken string) (*AuthBrokerCredentialValue, error) {
	backoff := gax.Backoff{
		Initial:    1 * time.Millisecond,
		Max:        5 * time.Second,
		Multiplier: 2,
	}

	var err error
	for {
		if ctxErr := ctx.Err(); ctxErr != nil {
			if err != nil {
				return nil, fmt.Errorf("auth broker credentials fetch aborted: %w (last attempt: %w)", ctxErr, err)
			}
			return nil, ctxErr
		}

		var creds *AuthBrokerCredentialValue
		creds, err = b.fetchCredentials(ctx, identityToken)
		if err == nil {
			return creds, nil
		}

		if !errors.Is(err, ErrRetryableAuthBroker) {
			return nil, err
		}

		if sleepErr := gax.Sleep(ctx, backoff.Pause()); sleepErr != nil {
			return nil, fmt.Errorf("auth broker credentials fetch aborted: %w (last attempt: %w)", sleepErr, err)
		}
	}
}

func (b *AuthBrokerCredentials) fetchCredentials(ctx context.Context, identityToken string) (*AuthBrokerCredentialValue, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request to auth broker: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", identityToken))

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRetryableAuthBroker, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("%w: auth broker returned status %d", ErrRetryableAuthBroker, resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth broker returned non-200 status: %d", resp.StatusCode)
	}

	var creds AuthBrokerCredentialValue
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("failed to decode auth broker response: %w", err)
	}

	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" || creds.SessionToken == "" || creds.Expiration.IsZero() {
		return nil, errors.New("auth broker response missing required fields (access_key_id, secret_access_key, session_token, expiration)")
	}

	return &creds, nil
}

func (b *AuthBrokerCredentials) readIdentityToken() (string, error) {
	tok, err := os.ReadFile(b.identityTokenPath)
	if err != nil {
		return "", fmt.Errorf("failed to read web identity token from %q: %w", b.identityTokenPath, err)
	}
	// An empty file most likely means we caught kubelet mid-rotation. Fail
	// clearly at this layer rather than sending "Authorization: Bearer " to
	// the broker and getting an opaque 401 back.
	trimmed := strings.TrimSpace(string(tok))
	if trimmed == "" {
		return "", fmt.Errorf("web identity token file %q is empty", b.identityTokenPath)
	}
	return trimmed, nil
}

var _ credentials.Provider = (*AuthBrokerCredentials)(nil)
