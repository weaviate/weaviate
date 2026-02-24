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

package gcpcommon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

type AuthBrokerTokenSource struct {
	endpoint string
	client   *http.Client
}

const maxRetries = 3

var (
	_                      oauth2.TokenSource = (*AuthBrokerTokenSource)(nil)
	httpClientTimeout                         = 5 * time.Second
	ErrRetryableAuthBroker                    = errors.New("retryable error from auth broker")
)

type AuthBrokerToken struct {
	AccessToken string    `json:"access_token"`
	Expiry      time.Time `json:"expiry"`
	TokenType   string    `json:"token_type"`
}

func NewAuthBrokerTokenSource(endpoint string) *AuthBrokerTokenSource {
	return &AuthBrokerTokenSource{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: httpClientTimeout,
		},
	}
}

func (b *AuthBrokerTokenSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	identityToken, err := b.getIdentityToken(ctx)
	if err != nil {
		return nil, err
	}

	return b.fetchTokenWithRetry(ctx, identityToken)
}

func (b *AuthBrokerTokenSource) fetchTokenWithRetry(ctx context.Context, identityToken string) (*oauth2.Token, error) {
	backoff := gax.Backoff{
		Initial:    200 * time.Millisecond,
		Max:        2 * time.Second,
		Multiplier: 2,
	}

	var err error
	for range maxRetries {
		var tok *oauth2.Token
		tok, err = b.fetchToken(ctx, identityToken)
		if err == nil {
			return tok, nil
		}

		if !errors.Is(err, ErrRetryableAuthBroker) {
			return nil, err
		}

		if sleepErr := gax.Sleep(ctx, backoff.Pause()); sleepErr != nil {
			return nil, err
		}
	}

	return nil, err
}

func (b *AuthBrokerTokenSource) fetchToken(ctx context.Context, identityToken string) (*oauth2.Token, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to created request to auth broker: %w", ErrRetryableAuthBroker, err)
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

	var bt AuthBrokerToken
	if err := json.NewDecoder(resp.Body).Decode(&bt); err != nil {
		return nil, fmt.Errorf("failed to decode auth broker response: %w", err)
	}

	return &oauth2.Token{
		AccessToken: bt.AccessToken,
		TokenType:   bt.TokenType,
		Expiry:      bt.Expiry,
	}, nil
}

func (b *AuthBrokerTokenSource) getIdentityToken(ctx context.Context) (string, error) {
	if !metadata.OnGCE() {
		return "", errors.New("cluster is not running on GCE/GKE")
	}

	tok, err := metadata.GetWithContext(
		ctx,
		fmt.Sprintf("instance/service-accounts/default/identity?audience=%s&format=full", b.endpoint),
	)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get identity token")
	}

	return tok, nil
}
