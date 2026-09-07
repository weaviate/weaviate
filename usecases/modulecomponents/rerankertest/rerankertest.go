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

// Package rerankertest holds test scaffolding shared by the reranker module
// client/config test suites (reranker-jinaai, reranker-voyageai, ...). Each
// provider's HTTP wire format differs (field names, error envelopes), but the
// surrounding test structure — a thread-safe stub server that dispatches
// batched requests, the assertions proving batching reassembles results in
// order, a moduletools.ClassConfig test double, the "default baseURL
// unchanged, override honored" check, and the SSRF-rejection table for a
// baseURL class-setting — is identical across providers. Factoring it here
// once, instead of once per module, is what this package is for.
//
// It is intentionally not a _test.go file: that suffix would make it
// invisible to every package other than its own, and these helpers exist
// specifically to be imported from other modules' external test packages.
package rerankertest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
	weaviateconfig "github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

// FakeClassConfig is a minimal moduletools.ClassConfig test double for
// reranker tests that only need Class()/ClassByModuleName() to return a fixed
// map (typically just {"baseURL": ...}, sometimes with "model" too).
type FakeClassConfig struct {
	ClassConfig map[string]interface{}
}

func (f FakeClassConfig) Class() map[string]interface{} {
	return f.ClassConfig
}

func (f FakeClassConfig) ClassByModuleName(string) map[string]interface{} {
	return f.ClassConfig
}

func (f FakeClassConfig) Property(string) map[string]interface{} {
	return nil
}

func (f FakeClassConfig) Tenant() string {
	return ""
}

func (f FakeClassConfig) TargetVector() string {
	return ""
}

func (f FakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return nil
}

func (f FakeClassConfig) Config() *weaviateconfig.Config {
	return nil
}

// SSRFTestCase is one row of the standard SSRF-rejection table used to
// validate a module's baseURL class-setting.
type SSRFTestCase struct {
	Name    string
	BaseURL string
	WantErr bool
}

// SSRFTestCases returns the SSRF-rejection cases (loopback, private-network,
// localhost, .local suffix, empty host, http scheme) that every reranker
// module's Test_classSettings_ValidateBaseURL exercises identically against
// usecases/modulecomponents.ValidateBaseURL. Callers append their own
// module-specific "default URL is valid" case.
func SSRFTestCases() []SSRFTestCase {
	return []SSRFTestCase{
		{Name: "valid HTTPS URL", BaseURL: "https://api.openai.com", WantErr: false},
		{Name: "HTTP URL is rejected", BaseURL: "http://api.example.com", WantErr: true},
		{Name: "loopback address is rejected", BaseURL: "https://127.0.0.1", WantErr: true},
		{Name: "private network address is rejected", BaseURL: "https://192.168.1.1", WantErr: true},
		{Name: "empty host is rejected", BaseURL: "https://", WantErr: true},
		{Name: "localhost is rejected", BaseURL: "https://localhost", WantErr: true},
		{Name: "local domain is rejected", BaseURL: "https://myhost.local", WantErr: true},
	}
}

// AssertBaseURLOverride proves a reranker client's URL builder (1) returns
// the module's hardcoded default host unchanged when no override is present,
// and (2) honors a context-supplied X-*-Baseurl header override. Every
// reranker client needs exactly this two-case check against its own default
// host, header key, and expected paths.
func AssertBaseURLOverride(
	t *testing.T,
	buildURL func(ctx context.Context, baseURL string) (string, error),
	defaultBaseURL, wantDefaultURL string,
	headerKey, overrideBaseURL, wantOverrideURL string,
) {
	t.Helper()
	ctx := context.Background()

	url, err := buildURL(ctx, defaultBaseURL)
	assert.NoError(t, err)
	assert.Equal(t, wantDefaultURL, url)

	ctxWithBaseURL := context.WithValue(ctx, headerKey, []string{overrideBaseURL}) //nolint:staticcheck // matches the string-keyed header lookup the clients use
	url, err = buildURL(ctxWithBaseURL, defaultBaseURL)
	assert.NoError(t, err)
	assert.Equal(t, wantOverrideURL, url)
}

// ResultItem is the minimal shape every reranker provider's per-result wire
// type exposes, letting Handler build and inspect responses without knowing
// whether the concrete type is jinaai's Result, voyageai's Data, or another
// provider's equivalent.
type ResultItem interface {
	ResultIndex() int
	ResultScore() float64
}

// requestDocs decodes just the "documents" field of a rank request body,
// ignoring every provider-specific field (model, truncation, ...) that
// surrounds it.
type requestDocs struct {
	Documents []string `json:"documents"`
}

// BatchIndex picks which of the four stubbed batch responses a request
// should receive, based on the "Response N" markers the reranker client
// tests' documents fixture uses to fan a 7-document request out into 4
// concurrent 2-document chunks.
func BatchIndex(documents []string) int {
	contains := func(doc string) bool {
		for _, d := range documents {
			if d == doc {
				return true
			}
		}
		return false
	}
	switch {
	case contains("Response 7"):
		return 3
	case contains("Response 5"):
		return 2
	case contains("Response 3"):
		return 1
	default:
		return 0
	}
}

// Handler is a thread-safe httptest.Handler double for the reranker family's
// rank endpoint. Serializing the response body is inherently
// provider-specific (different field names, different error envelopes), so
// BuildResponse/BuildError are supplied by the caller; everything else —
// locking, decoding the request, and picking which batched response to serve
// based on which documents came in — is identical across providers.
type Handler[T ResultItem] struct {
	lock sync.RWMutex

	t              *testing.T
	results        []T
	batchedResults [][]T
	errorMessage   string
	buildResponse  func(results []T) ([]byte, error)
	buildError     func(message string) []byte
}

// NewHandler builds a Handler. Pass either results (a single-batch response)
// or batchedResults (dispatched via BatchIndex), plus errorMessage to make
// every request fail with that message instead of returning a response.
func NewHandler[T ResultItem](
	t *testing.T,
	results []T,
	batchedResults [][]T,
	errorMessage string,
	buildResponse func(results []T) ([]byte, error),
	buildError func(message string) []byte,
) *Handler[T] {
	return &Handler[T]{
		t:              t,
		results:        results,
		batchedResults: batchedResults,
		errorMessage:   errorMessage,
		buildResponse:  buildResponse,
		buildError:     buildError,
	}
}

func (h *Handler[T]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.errorMessage != "" {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(h.buildError(h.errorMessage))
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(h.t, err)
	defer r.Body.Close()

	var req requestDocs
	require.Nil(h.t, json.Unmarshal(bodyBytes, &req))

	results := h.results
	if len(h.batchedResults) > 0 {
		results = h.batchedResults[BatchIndex(req.Documents)]
	}

	outBytes, err := h.buildResponse(results)
	require.Nil(h.t, err)

	w.Write(outBytes)
}

// AssertBatchScores checks that a batched Rank response preserves
// per-document order and that the first and last document scores match the
// first and last stubbed batch response (proving requests were dispatched to
// the right goroutine and reassembled in order) — the exact invariant every
// reranker client's batching test needs to prove, regardless of provider.
func AssertBatchScores(t *testing.T, documents []string, resp *ent.RankResult, firstScore, lastScore float64) {
	t.Helper()
	require.NotNil(t, resp)
	require.NotNil(t, resp.DocumentScores)
	for i := range resp.DocumentScores {
		assert.Equal(t, documents[i], resp.DocumentScores[i].Document)
		if i == 0 {
			assert.Equal(t, firstScore, resp.DocumentScores[i].Score)
		}
		if i == len(documents)-1 {
			assert.Equal(t, lastScore, resp.DocumentScores[i].Score)
		}
	}
}
