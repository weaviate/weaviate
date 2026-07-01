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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type AuthBrokerCredentials struct {
	credentials.Expiry
	endpoint string
	client   *http.Client
}

type AuthBrokerCredentialValue struct {
	AccessKeyID     string    `json:"access_key_id"`
	SecretAccessKey string    `json:"secret_access_key"`
	SessionToken    string    `json:"session_token"`
	Expiration      time.Time `json:"expiration"`
}

const maxRetries = 5

var (
	httpClientTimeout      = 5 * time.Second
	ErrRetryableAuthBroker = errors.New("retryable error from auth broker")
)

func NewAuthBrokerCredentials(endpoint string) *AuthBrokerCredentials {
	return &AuthBrokerCredentials{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: httpClientTimeout,
		},
	}
}

func (b *AuthBrokerCredentials) Retrieve() (credentials.Value, error) {
	return b.RetrieveWithCredContext(nil)
}

func (b *AuthBrokerCredentials) RetrieveWithCredContext(_ *credentials.CredContext) (credentials.Value, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	identityToken, err := b.getIdentityToken()
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
		Initial:    500 * time.Millisecond,
		Max:        3 * time.Second,
		Multiplier: 2,
	}

	var err error
	for range maxRetries {
		var creds *AuthBrokerCredentialValue
		creds, err = b.fetchCredentials(ctx, identityToken)
		if err == nil {
			return creds, nil
		}

		if !errors.Is(err, ErrRetryableAuthBroker) {
			return nil, err
		}

		if gax.Sleep(ctx, backoff.Pause()) != nil {
			return nil, err
		}
	}

	return nil, err
}

func (b *AuthBrokerCredentials) fetchCredentials(ctx context.Context, identityToken string) (*AuthBrokerCredentialValue, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create request to auth broker: %w", ErrRetryableAuthBroker, err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", identityToken))

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRetryableAuthBroker, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("%w: auth broker returned status %d", ErrRetryableAuthBroker, resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth broker returned non-200 status: %d", resp.StatusCode)
	}

	var creds AuthBrokerCredentialValue
	if err := json.NewDecoder(resp.Body).Decode(&creds); err != nil {
		return nil, fmt.Errorf("failed to decode auth broker response: %w", err)
	}

	return &creds, nil
}

func (b *AuthBrokerCredentials) getIdentityToken() (string, error) {
	path := os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
	if path == "" {
		return "", errors.New("AWS_WEB_IDENTITY_TOKEN_FILE not set; auth broker requires an IRSA web identity token")
	}

	tok, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read web identity token from %q: %w", path, err)
	}

	return strings.TrimSpace(string(tok)), nil
}

var _ credentials.Provider = (*AuthBrokerCredentials)(nil)
