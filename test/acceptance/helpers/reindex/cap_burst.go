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

package reindexhelpers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/entities/models"
)

// WithStatus500Fatal overrides the fatal message used by
// AssertCapBurstOutcome when a burst submit surfaces a 500. Used by the
// follower-forwarded multinode variant, where a 500 means the
// [dtm-perm/task-cap-exceeded] marker was lost on the follower→leader gRPC
// round-trip instead of a plain unexpected-status failure.
func WithStatus500Fatal(msg string) Option {
	return func(o *options) { o.status500Fatal = msg }
}

// CapBurstProps builds n text properties named prop_00..prop_{n-1} — the
// property set of the parallel cap-burst tests: one text property per
// submit; IndexFilterable=false so {"filterable":{"enabled":true}} passes
// validation for every property.
func CapBurstProps(n int) []*models.Property {
	props := make([]*models.Property, 0, n)
	for i := 0; i < n; i++ {
		props = append(props, &models.Property{
			Name:            fmt.Sprintf("prop_%02d", i),
			DataType:        []string{"text"},
			Tokenization:    "word",
			IndexFilterable: BoolPtr(false),
			IndexSearchable: BoolPtr(true),
		})
	}
	return props
}

// FireIndexUpdateRaw fires PUT /v1/schema/{collection}/indexes/{property}
// with the supplied JSON body and reports the raw outcome. It performs no
// assertions, so it is safe to call from errgroup goroutines.
func FireIndexUpdateRaw(client *http.Client, restURI, collection, property, jsonBody string) (int, string, error) {
	url := fmt.Sprintf("http://%s/v1/schema/%s/indexes/%s", restURI, collection, property)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader([]byte(jsonBody)))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

// IndexUpdateBurstResult is the per-property outcome of one submit in a
// FireIndexUpdateBurst burst.
type IndexUpdateBurstResult struct {
	Property   string
	StatusCode int
	Body       string
	Err        error
}

// FireIndexUpdateBurst fires n submits of
// PUT /v1/schema/{collection}/indexes/prop_%02d with jsonBody against
// restURI as a truly parallel burst and returns the per-property results in
// submission order.
//
// Start barrier: every goroutine blocks on the gate until it is closed, so
// all requests leave as simultaneously as the HTTP stack allows. No
// assertions inside the goroutines (require.* must run on the test
// goroutine); transport errors are reported per result. The returned error
// is the errgroup result — nil by construction, asserted by callers.
func FireIndexUpdateBurst(client *http.Client, restURI, collection, jsonBody string, n int) ([]IndexUpdateBurstResult, error) {
	gate := make(chan struct{})
	results := make([]IndexUpdateBurstResult, n)

	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			<-gate
			prop := fmt.Sprintf("prop_%02d", i)
			status, body, err := FireIndexUpdateRaw(client, restURI, collection, prop, jsonBody)
			results[i] = IndexUpdateBurstResult{Property: prop, StatusCode: status, Body: body, Err: err}
			return nil
		})
	}
	close(gate)
	return results, g.Wait()
}

// AssertCapBurstOutcome asserts the outcome of a cap-burst: exactly
// expectedCap submits accepted (202) and expectedOverCap rejected (429).
// Each 429 body must decode to the standard models.ErrorResponse shape and
// name the capped collection; transport errors and any other status are
// fatal. WithStatus500Fatal overrides the 500 fatal text. Returns the
// observed accepted count for use in subsequent settle polls.
func AssertCapBurstOutcome(t *testing.T, results []IndexUpdateBurstResult, collection string, expectedCap, expectedOverCap int, opts ...Option) int {
	t.Helper()
	o := applyOptions(opts)

	var accepted, tooMany int
	for _, res := range results {
		require.NoError(t, res.Err, "request for %s failed at transport level", res.Property)
		switch res.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusTooManyRequests:
			tooMany++
			var errResp models.ErrorResponse
			require.NoError(t, json.Unmarshal([]byte(res.Body), &errResp),
				"429 body must decode to the standard ErrorResponse shape: %s", res.Body)
			require.NotEmpty(t, errResp.Error)
			assert.Contains(t, errResp.Error[0].Message, collection,
				"429 message must name the capped collection")
		default:
			if res.StatusCode == http.StatusInternalServerError && o.status500Fatal != "" {
				t.Fatalf("500 for %s — %s: %s", res.Property, o.status500Fatal, res.Body)
			}
			t.Fatalf("unexpected status %d for %s: %s", res.StatusCode, res.Property, res.Body)
		}
	}
	assert.Equal(t, expectedCap, accepted,
		"exactly the cap may be admitted, no matter how parallel the burst")
	assert.Equal(t, expectedOverCap, tooMany,
		"the over-cap remainder must be rejected with 429")
	return accepted
}
