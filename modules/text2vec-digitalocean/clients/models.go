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

package clients

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/ent"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

// modelCacheTTL controls how long a successful /v1/models response is cached
// per (baseURL, apiKey-hash). The DigitalOcean model catalogue changes rarely
// and Validate is called every time a collection is created or its module
// config changes, so a short TTL is a reasonable trade-off between freshness
// and avoiding excessive calls.
const modelCacheTTL = 5 * time.Minute

type modelListResponse struct {
	Object string             `json:"object"`
	Data   []modelListItem    `json:"data"`
	Error  *digitalOceanError `json:"error,omitempty"`
}

type modelListItem struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
	Created int64  `json:"created"`
}

type modelCacheEntry struct {
	models    []string
	fetchedAt time.Time
}

// ModelLister fetches the model catalogue from a DigitalOcean Serverless
// Inference endpoint and caches the result in-memory.
type ModelLister struct {
	httpClient *http.Client

	mu    sync.Mutex
	cache map[string]modelCacheEntry
}

func NewModelLister(timeout time.Duration) *ModelLister {
	return &ModelLister{
		httpClient: modulecomponents.NewBaseHttpClient(timeout),
		cache:      map[string]modelCacheEntry{},
	}
}

func (l *ModelLister) ListModels(ctx context.Context, baseURL, apiKey, weaviateUUID string) ([]string, error) {
	cacheKey := l.cacheKey(baseURL, apiKey)

	l.mu.Lock()
	if entry, ok := l.cache[cacheKey]; ok && time.Since(entry.fetchedAt) < modelCacheTTL {
		l.mu.Unlock()
		return entry.models, nil
	}
	l.mu.Unlock()

	models, err := l.fetch(ctx, baseURL, apiKey, weaviateUUID)
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	l.cache[cacheKey] = modelCacheEntry{models: models, fetchedAt: time.Now()}
	l.mu.Unlock()

	return models, nil
}

func (l *ModelLister) fetch(ctx context.Context, baseURL, apiKey, weaviateUUID string) ([]string, error) {
	endpoint, err := url.JoinPath(baseURL, "/v1/models")
	if err != nil {
		return nil, errors.Wrap(err, "build /v1/models URL")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create GET request")
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	req.Header.Set("Content-Type", "application/json")
	if weaviateUUID != "" {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/%s %s", weaviateUUID, build.Version))
	} else {
		req.Header.Set("User-Agent", fmt.Sprintf("vector-db/weaviate/unknown %s", build.Version))
	}

	res, err := l.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "send GET request")
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read response body")
	}

	var parsed modelListResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse /v1/models response (status %d): %w", res.StatusCode, err)
	}

	if res.StatusCode != http.StatusOK || parsed.Error != nil {
		msg := fmt.Sprintf("DigitalOcean /v1/models returned status %d", res.StatusCode)
		if parsed.Error != nil && parsed.Error.Message != "" {
			msg = fmt.Sprintf("%s: %s", msg, parsed.Error.Message)
		}
		return nil, errors.New(msg)
	}

	ids := make([]string, 0, len(parsed.Data))
	for _, m := range parsed.Data {
		if m.ID == "" {
			continue
		}
		ids = append(ids, m.ID)
	}
	return ids, nil
}

func (l *ModelLister) cacheKey(baseURL, apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%s|%x", baseURL, h)
}

// init registers the default model lister with the ent package so that
// ValidateClass can call out to DigitalOcean without ent depending on this
// package.
func init() {
	ent.DefaultModelLister = NewModelLister(30 * time.Second)
}
