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

package clients

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func FuzzVectorizeTotal(f *testing.F) {
	f.Add("some input", "ada", "org1", "api-key-1", "base.url", "deployID", "resourceX", true, "2024-04-01")
	f.Fuzz(func(t *testing.T, text, model, org, key, baseURL, deployID, resource string, isAzure bool, apiVersion string) {
		if text == "" || model == "" || key == "" {
			t.Skip("incomplete input")
			return
		}

		embedding := make([]float32, rand.Intn(20)+1)
		for i := range embedding {
			embedding[i] = rand.Float32()
		}

		dataEntry := map[string]interface{}{
			"object":    text,
			"index":     0,
			"embedding": embedding,
		}

		resBody := map[string]interface{}{
			"object": "list",
			"data":   []interface{}{dataEntry},
			"usage": map[string]interface{}{
				"prompt_tokens":     rand.Intn(1000),
				"completion_tokens": rand.Intn(1000),
			},
		}

		if rand.Float32() < 0.2 {
			resBody["error"] = map[string]interface{}{
				"message": "main error",
				"type":    "fail",
				"param":   "some",
				"code":    500,
			}
			// must still include valid data to avoid crash
			resBody["data"] = []interface{}{dataEntry}
		}

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("x-ratelimit-limit-requests", strconv.Itoa(rand.Intn(1000)))
			w.Header().Set("x-ratelimit-limit-tokens", strconv.Itoa(rand.Intn(1000)))
			w.Header().Set("x-ratelimit-remaining-requests", strconv.Itoa(rand.Intn(1000)))
			w.Header().Set("x-ratelimit-remaining-tokens", strconv.Itoa(rand.Intn(1000)))

			if rand.Float32() < 0.05 {
				w.Write([]byte("{ broken"))
				return
			}

			_ = json.NewEncoder(w).Encode(resBody)
		}))
		defer server.Close()

		ctx := context.Background()
		ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{key})
		ctx = context.WithValue(ctx, "X-Openai-Organization", []string{org})
		ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{baseURL})
		ctx = context.WithValue(ctx, "X-Azure-Deployment-Id", []string{deployID})
		ctx = context.WithValue(ctx, "X-Azure-Resource-Name", []string{resource})
		ctx = context.WithValue(ctx, "X-Openai-Ratelimit-RequestPM-Embedding", []string{strconv.Itoa(rand.Intn(1000))})
		ctx = context.WithValue(ctx, "X-Openai-Ratelimit-TokenPM-Embedding", []string{strconv.Itoa(rand.Intn(1000))})

		cfg := fakeClassConfig{classConfig: map[string]interface{}{
			"Type":         "text",
			"Model":        model,
			"IsAzure":      isAzure,
			"ApiVersion":   apiVersion,
			"BaseURL":      baseURL,
			"ResourceName": resource,
			"DeploymentID": deployID,
		}}

		c := New(key, org, key, time.Second, nullLogger())
		c.buildUrlFn = func(_, _, _, _ string, _ bool) (string, error) {
			return server.URL, nil
		}

		_, _, _, _ = c.Vectorize(ctx, []string{text}, cfg)
		_, _ = c.VectorizeQuery(ctx, []string{text}, cfg)
		_ = c.GetApiKeyHash(ctx, cfg)
		_ = c.GetVectorizerRateLimit(ctx, cfg)
	})
}
