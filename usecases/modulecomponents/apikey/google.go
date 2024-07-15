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

package apikey

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/usecases/modulecomponents"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type GoogleApiKey struct {
	mutex sync.RWMutex
	token *oauth2.Token
}

func NewGoogleApiKey() *GoogleApiKey {
	return &GoogleApiKey{}
}

func (g *GoogleApiKey) GetApiKey(ctx context.Context, envApiKeyValue string, useGenerativeAIEndpoint, useGoogleAuth bool) (string, error) {
	if useGenerativeAIEndpoint {
		if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Google-Studio-Api-Key"); apiKey != "" {
			return apiKey, nil
		}
	} else {
		if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Google-Vertex-Api-Key"); apiKey != "" {
			return apiKey, nil
		}
	}
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Google-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if apiKey := modulecomponents.GetValueFromContext(ctx, "X-Palm-Api-Key"); apiKey != "" {
		return apiKey, nil
	}
	if !useGenerativeAIEndpoint && useGoogleAuth {
		return g.getAuthToken(ctx)
	}
	if envApiKeyValue != "" {
		return envApiKeyValue, nil
	}
	return "", errors.New("no api key found " +
		"neither in request header: X-Palm-Api-Key or X-Google-Api-Key or X-Google-Vertex-Api-Key or X-Google-Studio-Api-Key " +
		"nor in environment variable under PALM_APIKEY or GOOGLE_APIKEY")
}

func (g *GoogleApiKey) getAuthToken(ctx context.Context) (string, error) {
	if accessToken := g.getAccessToken(); accessToken != "" {
		return accessToken, nil
	}
	return g.updateAndGetAccessToken(ctx)
}

func (g *GoogleApiKey) updateAndGetAccessToken(ctx context.Context) (string, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	// This method checks all possible places for Google credentials and if successful gets the token source
	// It should only be used with Vertex AI models
	// Uses scope: https://cloud.google.com/iam/docs/create-short-lived-credentials-direct
	tokenSource, err := google.DefaultTokenSource(ctx, "https://www.googleapis.com/auth/cloud-platform")
	if err != nil {
		return "", fmt.Errorf("unable to find Google credentials: %w", err)
	}
	token, err := tokenSource.Token()
	if err != nil {
		return "", fmt.Errorf("unable to obtain Google token: %w", err)
	}
	g.token = token
	return token.AccessToken, nil
}

func (g *GoogleApiKey) getAccessToken() string {
	g.mutex.RLock()
	defer g.mutex.RUnlock()

	if g.token != nil && g.token.Valid() {
		return g.token.AccessToken
	}
	return ""
}
