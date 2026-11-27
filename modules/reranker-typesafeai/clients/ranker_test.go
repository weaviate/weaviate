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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	churn          = "The customer is about to churn"
	noRefund       = "Three weeks and still no refund. Nobody answers my emails."
	chargedTwice   = "I was charged twice and the refund never came."
	changeEmail    = "How do I change the email on my account?"
	testTimeout    = 30 * time.Second
	defaultFakeKey = "apiKey"
)

func TestRankSingleDocument(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.93}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, &ent.RankResult{Query: churn, DocumentScores: []ent.DocumentScore{{Document: noRefund, Score: 0.93}}}, res)
	requests := f.received()
	require.Len(t, requests, 1)
	r := requests[0]
	assert.Equal(t, http.MethodPost, r.method)
	assert.Equal(t, "/v1/systemone", r.path)
	assert.Equal(t, "application/json", r.contentType)
	assert.Equal(t, "Bearer apiKey", r.authorization)
	assert.Empty(t, r.imagesHeader)
	assert.Equal(t, "jev-latest", r.body.Model)
	assert.Equal(t, noRefund, r.body.State)
	require.Len(t, r.body.Questions, 1)
	assert.Equal(t, wireQuestion{Type: "noul", Instructions: churn}, r.body.Questions["q"])
}

func TestRankPacksDocuments(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.91, chargedTwice: 0.64, changeEmail: 0.03}
	documents := []string{noRefund, chargedTwice, changeEmail}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		documents, classConfig(f.url(), map[string]any{"model": "jev-test"}))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{
		{Document: noRefund, Score: 0.91},
		{Document: chargedTwice, Score: 0.64},
		{Document: changeEmail, Score: 0.03},
	}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 1)
	body := requests[0].body
	assert.Equal(t, "jev-test", body.Model)
	assert.Equal(t, map[string]any{"document_0": noRefund, "document_1": chargedTwice, "document_2": changeEmail}, body.State)
	assert.Equal(t, map[string]wireQuestion{
		"document_0": {Type: "noul", Instructions: "In document_0: " + churn},
		"document_1": {Type: "noul", Instructions: "In document_1: " + churn},
		"document_2": {Type: "noul", Instructions: "In document_2: " + churn},
	}, body.Questions)
}

func TestRankBatchesByCount(t *testing.T) {
	tests := []struct {
		name      string
		documents []string
		wantSizes []int
	}{
		{name: "120 short documents", documents: numbered("ticket %d", 120), wantSizes: []int{20, 50, 50}},
		{
			name: "3 documents of 40,000 characters",
			documents: []string{
				strings.Repeat("a", 40_000), strings.Repeat("b", 40_000), strings.Repeat("c", 40_000),
			},
			wantSizes: []int{3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.probabilities = distinctProbabilities(tt.documents)

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				tt.documents, classConfig(f.url(), nil))

			require.NoError(t, err)
			assert.Equal(t, expectedScores(tt.documents, f.probabilities), res.DocumentScores)
			assert.Equal(t, tt.wantSizes, sizes(f.received()))
		})
	}
}

func TestRankHalvesARequestTheAPIRejectsForItsSize(t *testing.T) {
	t.Run("five documents", func(t *testing.T) {
		f := newFake(t)
		f.tooLarge = func(documents int) bool { return documents > 2 }
		documents := []string{noRefund, chargedTwice, changeEmail, "Cancel my plan today.", "Thanks, all good now."}
		f.probabilities = distinctProbabilities(documents)

		res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
			documents, classConfig(f.url(), nil))

		require.NoError(t, err)
		assert.Equal(t, expectedScores(documents, f.probabilities), res.DocumentScores)
		// 5 is rejected, then 2 and 3; 3 is rejected, then 1 and 2.
		assert.Equal(t, []int{1, 2, 2, 3, 5}, sizes(f.received()))
	})

	rejectedAtEverySize := []struct {
		name         string
		documents    []string
		wantRequests int
	}{
		{name: "a single document", documents: []string{noRefund}, wantRequests: 1},
		// 2 is rejected, then 1, the first document; the second is not sent.
		{name: "two documents", documents: []string{noRefund, chargedTwice}, wantRequests: 2},
	}
	for _, tt := range rejectedAtEverySize {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.tooLarge = func(int) bool { return true }

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				tt.documents, classConfig(f.url(), nil))

			require.EqualError(t, err,
				`connection to TypeSafe AI API failed with status 400: {"error_type":"max_tokens_exceeded"}`)
			assert.Len(t, f.received(), tt.wantRequests)
		})
	}
}

func TestRankReturnsThe400ToADocumentASplitIsolated(t *testing.T) {
	f := newFake(t)
	f.tooLarge = func(documents int) bool { return documents > 1 }
	f.replies = map[int]fakeReply{1: {status: http.StatusBadRequest, body: `{"error":"unsupported content"}`}}

	_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund, chargedTwice}, classConfig(f.url(), nil))

	require.EqualError(t, err, `connection to TypeSafe AI API failed with status 400: {"error":"unsupported content"}`)
	assert.Equal(t, []int{1, 2}, sizes(f.received()))
}

func TestRankScoresAWhitespaceOnlyDocumentZeroWithoutSendingIt(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"a": 0.8, "b": 0.2}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{" ", "a", "\t\n ", "b"}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{
		{Document: " ", Score: 0},
		{Document: "a", Score: 0.8},
		{Document: "\t\n ", Score: 0},
		{Document: "b", Score: 0.2},
	}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, map[string]any{"document_0": "a", "document_1": "b"}, requests[0].body.State)
}

func TestRankSkipsEmptyAndDuplicateDocuments(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"a": 0.8, "b": 0.2}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{"", "a", "a", "b"}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{
		{Document: "", Score: 0},
		{Document: "a", Score: 0.8},
		{Document: "a", Score: 0.8},
		{Document: "b", Score: 0.2},
	}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, map[string]any{"document_0": "a", "document_1": "b"}, requests[0].body.State)
}

func TestRankWithEveryDocumentEmpty(t *testing.T) {
	f := newFake(t)

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{"", ""}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: "", Score: 0}, {Document: "", Score: 0}}, res.DocumentScores)
	assert.Empty(t, f.received())
}

func TestRankRequiresAQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "no query", query: ""},
		{name: "a space", query: " "},
		{name: "whitespace", query: " \t\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), tt.query, "text",
				[]string{noRefund}, classConfig(f.url(), nil))

			require.EqualError(t, err, "rerank.query is required by reranker-typesafeai: the query is the condition the model judges")
			assert.Empty(t, f.received())
		})
	}
}

func TestRankRejectsAPropertyOfAnotherType(t *testing.T) {
	f := newFake(t)
	tests := []struct {
		name     string
		property string
		cfg      moduletools.ClassConfig
		wantErr  string
	}{
		{
			name:     "a text[] property",
			property: "tags",
			cfg:      classConfig(f.url(), nil),
			wantErr:  `property "tags" is not a text or blob property; reranker-typesafeai reranks text and blob properties`,
		},
		{
			name:     "a property the class does not have",
			property: "missing",
			cfg:      classConfig(f.url(), nil),
			wantErr:  `property "missing" is not a text or blob property; reranker-typesafeai reranks text and blob properties`,
		},
		{
			name:     "no class config",
			property: "text",
			cfg:      nil,
			wantErr:  "no class config",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, tt.property,
				[]string{noRefund}, tt.cfg)

			require.EqualError(t, err, tt.wantErr)
		})
	}
	assert.Empty(t, f.received())
}

// A class created before v1.19 and never changed still stores the deprecated
// string type; the gRPC check reads it as text, so the module does too.
func TestRankReadsTheDeprecatedStringTypeAsText(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.9}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "legacy",
		[]string{noRefund}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: noRefund, Score: 0.9}}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, noRefund, requests[0].body.State)
}

func TestRankFailsAtOnceOnProviderErrors(t *testing.T) {
	tests := []struct {
		name      string
		documents []string
		status    int
		body      string
		wantErr   string
	}{
		{
			name:      "429",
			documents: []string{noRefund},
			status:    http.StatusTooManyRequests,
			body:      `{"error":"rate limited"}`,
			wantErr:   `connection to TypeSafe AI API failed with status 429: {"error":"rate limited"}`,
		},
		{
			name:      "500",
			documents: []string{noRefund},
			status:    http.StatusInternalServerError,
			body:      `{"error":"internal error"}`,
			wantErr:   `connection to TypeSafe AI API failed with status 500: {"error":"internal error"}`,
		},
		{
			// The request holds the whole page, so the 400 can be about the
			// request, such as an unknown model, and names no object.
			name:      "400 to a page of one document",
			documents: []string{noRefund},
			status:    http.StatusBadRequest,
			body:      `{"error":"unsupported content"}`,
			wantErr:   `connection to TypeSafe AI API failed with status 400: {"error":"unsupported content"}`,
		},
		{
			name:      "400 to two documents",
			documents: []string{noRefund, chargedTwice},
			status:    http.StatusBadRequest,
			body:      `{"error":"unsupported content"}`,
			wantErr:   `connection to TypeSafe AI API failed with status 400: {"error":"unsupported content"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: tt.status, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				tt.documents, classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Len(t, f.received(), 1)
		})
	}
}

// One request more than a query sends at once. The fake answers 429 once the
// first ones have arrived, and the first failure cancels the query before it
// frees a slot, so the last request is never sent.
func TestRankStopsAfterTheFirstProviderError(t *testing.T) {
	f := newFake(t)
	f.together = _NUMCPU
	f.replies = map[int]fakeReply{}
	for i := range _NUMCPU + 1 {
		f.replies[i] = fakeReply{status: http.StatusTooManyRequests, body: `{"error":"rate limited"}`}
	}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		numbered("ticket %d", (_NUMCPU+1)*maxBatchDocuments), classConfig(f.url(), nil))

	require.EqualError(t, err, `connection to TypeSafe AI API failed with status 429: {"error":"rate limited"}`)
	assert.Nil(t, res)
	assert.Len(t, f.received(), _NUMCPU)
}

// A redirect could send the request, and with a 307 or 308 its key, to a host
// neither the class nor the request named.
func TestRankDoesNotFollowARedirect(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		fromHeader bool
		wantErr    string
	}{
		{name: "302", status: http.StatusFound, wantErr: "connection to TypeSafe AI API failed with status 302"},
		{name: "307", status: http.StatusTemporaryRedirect, wantErr: "connection to TypeSafe AI API failed with status 307"},
		{name: "308", status: http.StatusPermanentRedirect, wantErr: "connection to TypeSafe AI API failed with status 308"},
		{
			name:       "302 from a host the request names",
			status:     http.StatusFound,
			fromHeader: true,
			wantErr:    "connection to TypeSafe AI API failed with status 302",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := newFake(t)
			var redirected atomic.Int64
			redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				redirected.Add(1)
				w.Header().Set("Location", target.url()+apiPath)
				w.WriteHeader(tt.status)
			}))
			t.Cleanup(redirecting.Close)
			classBaseURL := redirecting.URL
			ctx := context.Background()
			if tt.fromHeader {
				classBaseURL = newFake(t).url()
				ctx = context.WithValue(ctx, "X-Typesafeai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Typesafeai-Baseurl", []string{redirecting.URL})
			}

			res, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classBaseURL, nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Equal(t, int64(1), redirected.Load())
			assert.Empty(t, target.received())
		})
	}
}

// The 429 and 500 rows of TestRankFailsAtOnceOnProviderErrors pin that a
// status is sent once.
func TestRankSendsARequestAgainWhenItsReusedConnectionBreaks(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.93}
	f.replies = map[int]fakeReply{1: {hangUp: true}}
	c := newTestClient(defaultFakeKey)
	want := &ent.RankResult{Query: churn, DocumentScores: []ent.DocumentScore{{Document: noRefund, Score: 0.93}}}

	// One document is one request, so the second call finds the connection of
	// the first in the pool.
	for call := range 2 {
		res, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))

		require.NoError(t, err, "call %d", call)
		assert.Equal(t, want, res, "call %d", call)
	}

	assert.Len(t, f.received(), 3, "the request of the second call is sent twice")
	assert.Equal(t, int64(2), f.opened.Load(), "the second call reused the connection of the first")
}

func TestRankHeaders(t *testing.T) {
	tests := []struct {
		name          string
		serverKey     string
		headerKey     string
		headerBaseURL bool
		wantErr       string
		wantAuth      string
	}{
		{
			name:      "key from the environment",
			serverKey: "env",
			wantAuth:  "Bearer env",
		},
		{
			name:      "header key wins",
			serverKey: "env",
			headerKey: "headerKey",
			wantAuth:  "Bearer headerKey",
		},
		{
			name:      "header key without a server key",
			headerKey: "headerKey",
			wantAuth:  "Bearer headerKey",
		},
		{
			name:          "base URL header with a key header and no server key",
			headerKey:     "headerKey",
			headerBaseURL: true,
			wantAuth:      "Bearer headerKey",
		},
		{
			name:          "base URL header with a key header",
			serverKey:     "env",
			headerKey:     "headerKey",
			headerBaseURL: true,
			wantAuth:      "Bearer headerKey",
		},
		{
			name: "no key anywhere",
			wantErr: "TypeSafe AI API key: no api key found neither in request header: X-Typesafeai-Api-Key " +
				"nor in environment variable under TYPESAFEAI_APIKEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			ctx := context.Background()
			if tt.headerKey != "" {
				ctx = context.WithValue(ctx, "X-Typesafeai-Api-Key", []string{tt.headerKey})
			}
			if tt.headerBaseURL {
				ctx = context.WithValue(ctx, "X-Typesafeai-Baseurl", []string{headerServer.url()})
			}

			_, err := newTestClient(tt.serverKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classServer.url(), nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, classServer.received())
				assert.Empty(t, headerServer.received())
				return
			}
			require.NoError(t, err)
			used, unused := classServer, headerServer
			if tt.headerBaseURL {
				used, unused = headerServer, classServer
			}
			assert.Empty(t, unused.received())
			requests := used.received()
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantAuth, requests[0].authorization)
		})
	}
}

func TestRankQuotesTheErrorBodyOfAHeaderBaseURL(t *testing.T) {
	f := newFake(t)
	f.replies = map[int]fakeReply{0: {status: http.StatusBadRequest, body: `{"error":"unknown model"}`}}
	ctx := context.WithValue(context.Background(), "X-Typesafeai-Baseurl", []string{f.url() + "/proxy"})
	ctx = context.WithValue(ctx, "X-Typesafeai-Api-Key", []string{"headerKey"})

	res, err := newTestClient("env").Rank(ctx, churn, "text", []string{noRefund}, classConfig(f.url(), nil))

	require.EqualError(t, err, `connection to TypeSafe AI API failed with status 400: {"error":"unknown model"}`)
	assert.Nil(t, res)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "/proxy/v1/systemone", requests[0].path)
	assert.Equal(t, "Bearer headerKey", requests[0].authorization)
}

func TestRankSendsNothingForABaseURLHeaderWithoutAScheme(t *testing.T) {
	f := newFake(t)
	ctx := context.WithValue(context.Background(), "X-Typesafeai-Api-Key", []string{"headerKey"})
	ctx = context.WithValue(ctx, "X-Typesafeai-Baseurl", []string{"api.typesafe.ai"})

	_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
		[]string{noRefund}, classConfig(f.url(), nil))

	require.EqualError(t, err, `send TypeSafe AI request: Post "api.typesafe.ai/v1/systemone": unsupported protocol scheme ""`)
	assert.Empty(t, f.received())
}

func TestRankValidatesABaseURLHeaderWhileBaseURLValidationIsOn(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	tests := []struct {
		name string
		// <fake> stands for the URL of the host the header names.
		baseURL string
		wantErr string
	}{
		{name: "http", baseURL: "<fake>", wantErr: "baseURL must use HTTPS"},
		{name: "http with /v1", baseURL: "<fake>/v1", wantErr: "baseURL must use HTTPS"},
		{name: "https to a loopback address", baseURL: "https://127.0.0.1:8443", wantErr: "baseURL cannot target internal addresses"},
		{name: "https to localhost", baseURL: "https://localhost/v1", wantErr: "baseURL cannot target internal addresses"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			ctx := context.WithValue(context.Background(), "X-Typesafeai-Api-Key", []string{"headerKey"})
			ctx = context.WithValue(ctx, "X-Typesafeai-Baseurl",
				[]string{strings.ReplaceAll(tt.baseURL, "<fake>", headerServer.url())})

			res, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classServer.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Empty(t, classServer.received())
			assert.Empty(t, headerServer.received())
		})
	}
}

// A class base URL may predate MODULES_VALIDATE_BASE_URL, so it is validated
// when a query uses it, before the key and the documents are sent.
func TestRankValidatesAClassBaseURLWhileBaseURLValidationIsOn(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	tests := []struct {
		name string
		// <fake> stands for the URL of the host the class names.
		baseURL string
		wantErr string
	}{
		{name: "http", baseURL: "<fake>", wantErr: "baseURL must use HTTPS"},
		{name: "http with /v1", baseURL: "<fake>/v1", wantErr: "baseURL must use HTTPS"},
		{name: "https to a loopback address", baseURL: "https://127.0.0.1:8443", wantErr: "baseURL cannot target internal addresses"},
		{name: "https to localhost", baseURL: "https://localhost/v1", wantErr: "baseURL cannot target internal addresses"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer := newFake(t)
			ctx := context.WithValue(context.Background(), "X-Typesafeai-Api-Key", []string{"headerKey"})

			res, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(strings.ReplaceAll(tt.baseURL, "<fake>", classServer.url()), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Empty(t, classServer.received())
		})
	}
}

func TestRankSendsNothingForAClassBaseURLWithoutAScheme(t *testing.T) {
	_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund}, classConfig("api.typesafe.ai", nil))

	require.EqualError(t, err, `send TypeSafe AI request: Post "api.typesafe.ai/v1/systemone": unsupported protocol scheme ""`)
}

// The cut of the quoted body drops bytes, and the search for the error_type
// reads the bytes before that cut.
func TestRankFindsTheErrorTypeInABodyThatIsNotUTF8(t *testing.T) {
	f := newFake(t)
	f.replies = map[int]fakeReply{0: {
		status: http.StatusBadRequest,
		body:   strings.Repeat("\xff", 500) + tooLargeError,
	}}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund, chargedTwice}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Len(t, res.DocumentScores, 2)
	assert.Equal(t, []int{1, 1, 2}, sizes(f.received()))
}

func TestRankKeepsTheBaseURLPath(t *testing.T) {
	f := newFake(t)

	_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund}, classConfig(f.url()+"/typesafe", nil))

	require.NoError(t, err)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "/typesafe/v1/systemone", requests[0].path)
}

// A query sends at most one request per CPU at once. The fake answers the
// requests in groups of that size, so the limit is reached.
func TestRankCapsRequestsInFlightPerQuery(t *testing.T) {
	images := make([]string, 2*_NUMCPU)
	for i := range images {
		images[i] = encodedImage(t, "png", 300+i, 300)
	}
	tests := []struct {
		name      string
		property  string
		documents []string
	}{
		{name: "text", property: "text", documents: numbered("ticket %d", 2*_NUMCPU*maxBatchDocuments)},
		{name: "images", property: "photo", documents: images},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.together = _NUMCPU

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, tt.property,
				tt.documents, classConfig(f.url(), nil))

			require.NoError(t, err)
			assert.Len(t, f.received(), 2*_NUMCPU)
			assert.Equal(t, _NUMCPU, f.maxInFlight())
		})
	}
}

func TestRankStopsOnContextCancel(t *testing.T) {
	// Twice as many requests as a query sends at once.
	documents := numbered("ticket %d", 2*_NUMCPU*maxBatchDocuments)
	f, release := newBlockingFake(t)
	defer release()
	c := newTestClient(defaultFakeKey)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := rankInBackground(t, ctx, c, f, documents)
	require.Eventually(t, func() bool { return len(f.received()) == _NUMCPU }, 5*time.Second, time.Millisecond)

	cancel()
	requireCanceledWithinASecond(t, result)

	release()
	waitFor(t, &f.handlers)
	assert.Len(t, f.received(), _NUMCPU, "the requests waiting for a slot are not sent")
	res, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))
	require.NoError(t, err)
	assert.Len(t, res.DocumentScores, 1)
}

// newBlockingFake returns a fake whose handlers wait for release. Defer
// release in the test: the calls' cleanups wait for the handlers.
func newBlockingFake(t *testing.T) (*fakeTypeSafeAI, func()) {
	f := newFake(t)
	f.release = make(chan struct{})
	return f, sync.OnceFunc(func() { close(f.release) })
}

func rankInBackground(t *testing.T, ctx context.Context, c *client, f *fakeTypeSafeAI, documents []string) <-chan error {
	var calls sync.WaitGroup
	t.Cleanup(calls.Wait)
	result := make(chan error, 1)
	calls.Go(func() {
		_, err := c.Rank(ctx, churn, "text", documents, classConfig(f.url(), nil))
		result <- err
	})
	return result
}

func requireCanceledWithinASecond(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Rank did not return within a second of the cancel")
	}
}

func TestRankChecksTheAnswers(t *testing.T) {
	tests := []struct {
		name      string
		documents []string
		body      string
		wantErr   string
	}{
		{
			name:      "no answers",
			documents: []string{noRefund},
			body:      `{"answers":{}}`,
			wantErr:   `TypeSafe AI response has no answers: {"answers":{}}`,
		},
		{
			name:      "an error in a 200",
			documents: []string{noRefund},
			body:      `{"error":"model overloaded"}`,
			wantErr:   `TypeSafe AI response has no answers: {"error":"model overloaded"}`,
		},
		{
			name:      "an error object in a 200",
			documents: []string{noRefund},
			body:      `{"detail":{"message":"model overloaded"}}`,
			wantErr:   `TypeSafe AI response has no answers: {"detail":{"message":"model overloaded"}}`,
		},
		{
			name:      "missing answer",
			documents: []string{noRefund},
			body:      `{"answers":{"document_0":{"type":"noul","noul":0.5}}}`,
			wantErr:   `TypeSafe AI response has no answer for "q"`,
		},
		{
			name:      "missing answer in a packed request",
			documents: []string{noRefund, chargedTwice},
			body:      `{"answers":{"document_0":{"type":"noul","noul":0.5},"document_2":{"type":"noul","noul":0.5}}}`,
			wantErr:   `TypeSafe AI response has no answer for "document_1"`,
		},
		{
			name:      "too few answers",
			documents: []string{noRefund, chargedTwice},
			body:      `{"answers":{"document_0":{"type":"noul","noul":0.5}}}`,
			wantErr:   "TypeSafe AI response has 1 answers for 2 documents",
		},
		{
			name:      "an extra answer",
			documents: []string{noRefund, chargedTwice},
			body: `{"answers":{"document_0":{"type":"noul","noul":0.5},"document_1":{"type":"noul","noul":0.5},` +
				`"document_7":{"type":"noul","noul":0.5}}}`,
			wantErr: "TypeSafe AI response has 3 answers for 2 documents",
		},
		{
			name:      "an extra answer to a single document",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"noul","noul":0.5},"document_0":{"type":"noul","noul":0.5}}}`,
			wantErr:   "TypeSafe AI response has 2 answers for 1 documents",
		},
		{
			name:      "answer of type score",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"score","score":0.5}}}`,
			wantErr:   `TypeSafe AI answer for "q" has type "score", want "noul"`,
		},
		{
			name:      "no noul value",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"noul"}}}`,
			wantErr:   `TypeSafe AI answer for "q" has no probability`,
		},
		{
			name:      "noul above 1",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"noul","noul":1.5}}}`,
			wantErr:   `TypeSafe AI answer for "q" has probability 1.5, outside [0, 1]`,
		},
		{
			name:      "noul below 0",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"noul","noul":-0.1}}}`,
			wantErr:   `TypeSafe AI answer for "q" has probability -0.1, outside [0, 1]`,
		},
		{
			name:      "not JSON",
			documents: []string{noRefund},
			body:      `<html>`,
			wantErr:   "parse TypeSafe AI response: invalid character '<' looking for beginning of value: <html>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				tt.documents, classConfig(f.url(), nil))

			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Len(t, f.received(), 1)
		})
	}
}

func TestRankDoesNotFailOnTheUsage(t *testing.T) {
	const answers = `"answers":{"q":{"type":"noul","noul":0.7}}`
	tests := []struct {
		name string
		body string
	}{
		{name: "a whole number", body: `{` + answers + `,"usage":{"input_tokens":120}}`},
		{name: "a fraction", body: `{` + answers + `,"usage":{"input_tokens":1.5}}`},
		{name: "a string", body: `{` + answers + `,"usage":{"input_tokens":"many"}}`},
		{name: "a usage that is not an object", body: `{` + answers + `,"usage":"n/a"}`},
		{name: "no usage", body: `{` + answers + `}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				[]string{noRefund}, classConfig(f.url(), nil))

			require.NoError(t, err)
			assert.Equal(t, []ent.DocumentScore{{Document: noRefund, Score: 0.7}}, res.DocumentScores)
		})
	}
}

func TestRankSendsABlobPropertyAsImages(t *testing.T) {
	const damaged = "The photo shows a damaged car"
	images := []string{encodedImage(t, "png", 300, 300), encodedImage(t, "jpeg", 320, 300)}
	f := newFake(t)
	f.probabilities = map[string]float64{images[0]: 0.9, images[1]: 0.2}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), damaged, "photo",
		images, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, expectedScores(images, f.probabilities), res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 2, "one request per image")
	got := map[string]wireAsset{}
	for _, r := range requests {
		assert.Equal(t, "true", r.imagesHeader)
		assert.Equal(t, "image_0", r.body.State)
		assert.Equal(t, wireQuestion{Type: "noul", Instructions: damaged}, r.body.Questions["q"])
		require.Len(t, r.body.Assets, 1)
		got[r.body.Assets["image_0"].DataBase64] = r.body.Assets["image_0"]
	}
	assert.Equal(t, map[string]wireAsset{
		images[0]: {Type: "image", DataBase64: images[0], Format: "png"},
		images[1]: {Type: "image", DataBase64: images[1], Format: "jpeg"},
	}, got)
}

func TestRankRejectsAnImageOutsideTheLimitsBeforeSending(t *testing.T) {
	valid, small := encodedImage(t, "png", 300, 300), encodedImage(t, "png", 100, 100)
	tests := []struct {
		name      string
		documents []string
		wantErr   string
	}{
		{
			name:      "at position 1",
			documents: []string{valid, small},
			wantErr:   "image: 100x100 pixels: the API takes 280 to 1568 pixels per side",
		},
		{
			name:      "after an empty document",
			documents: []string{valid, "", small},
			wantErr:   "image: 100x100 pixels: the API takes 280 to 1568 pixels per side",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), "The photo shows a damaged car", "photo",
				tt.documents, classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
		})
	}
}

func TestRankSendsBase64TextAsText(t *testing.T) {
	encoded := encodedImage(t, "png", 300, 300)
	f := newFake(t)
	f.probabilities = map[string]float64{encoded: 0.4}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{encoded}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: encoded, Score: 0.4}}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, encoded, requests[0].body.State)
	assert.Empty(t, requests[0].body.Assets)
	assert.Empty(t, requests[0].imagesHeader)
}

func nullLogger() logrus.FieldLogger {
	logger, _ := test.NewNullLogger()
	return logger
}

func newTestClient(apiKey string) *client {
	return New(apiKey, testTimeout, nullLogger())
}

func numbered(format string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i)
	}
	return out
}

func distinctProbabilities(documents []string) map[string]float64 {
	out := make(map[string]float64, len(documents))
	for i, document := range documents {
		out[document] = float64(i+1) / float64(len(documents)+1)
	}
	return out
}

func expectedScores(documents []string, probabilities map[string]float64) []ent.DocumentScore {
	out := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		out[i] = ent.DocumentScore{Document: document, Score: probabilities[document]}
	}
	return out
}

// sizes returns the number of documents of every request, sorted: requests
// run in parallel and arrive in any order.
func sizes(requests []fakeRequest) []int {
	out := make([]int, len(requests))
	for i, r := range requests {
		out[i] = r.documents()
	}
	slices.Sort(out)
	return out
}

func waitFor(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	var waiter sync.WaitGroup
	waiter.Go(func() {
		wg.Wait()
		close(done)
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the fake's handlers did not return")
	}
	waiter.Wait()
}

type wireQuestion struct {
	Type         string `json:"type"`
	Instructions string `json:"instructions"`
}

type wireAsset struct {
	Type       string `json:"type"`
	DataBase64 string `json:"data_base64"`
	Format     string `json:"format"`
}

// wireRequest decodes the request body with the API's field names,
// independent of the client's types.
type wireRequest struct {
	Model     string                  `json:"model"`
	State     any                     `json:"state"`
	Questions map[string]wireQuestion `json:"questions"`
	Assets    map[string]wireAsset    `json:"assets"`
}

type fakeRequest struct {
	method        string
	path          string
	contentType   string
	authorization string
	imagesHeader  string
	body          wireRequest
}

func (r fakeRequest) documents() int {
	if state, ok := r.body.State.(map[string]any); ok {
		return len(state)
	}
	return 1
}

type fakeReply struct {
	status int
	body   string
	// hangUp closes the connection without a response.
	hangUp bool
}

// fakeTypeSafeAI stands for POST /v1/systemone. It answers every question
// with the probability of its state value, or of the image the value refers
// to, 0.5 when unknown.
type fakeTypeSafeAI struct {
	t             *testing.T
	server        *httptest.Server
	probabilities map[string]float64
	// replies answers the n-th request, counted from 0, instead.
	replies map[int]fakeReply
	// tooLarge rejects a request of that many documents as the API rejects a
	// request above its token limit.
	tooLarge func(documents int) bool
	delay    time.Duration
	// together, when set, holds each handler until that many requests are in
	// their handlers, so every one of them needs a connection of its own.
	together int
	// release, when set, holds every handler until it is closed.
	release  chan struct{}
	handlers sync.WaitGroup

	// opened counts the connections the server accepted.
	opened atomic.Int64

	lock     sync.Mutex
	requests []fakeRequest
	inFlight int
	maxSeen  int
	// gathering closes when together requests have arrived; waiting counts
	// the requests that wait for it.
	gathering chan struct{}
	waiting   int
}

func newFake(t *testing.T) *fakeTypeSafeAI {
	f := &fakeTypeSafeAI{t: t}
	f.server = httptest.NewUnstartedServer(f)
	f.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.opened.Add(1)
		}
	}
	f.server.Start()
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeTypeSafeAI) url() string {
	return f.server.URL
}

func (f *fakeTypeSafeAI) received() []fakeRequest {
	f.lock.Lock()
	defer f.lock.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeTypeSafeAI) maxInFlight() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.maxSeen
}

func (f *fakeTypeSafeAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.handlers.Add(1)
	defer f.handlers.Done()

	var body wireRequest
	if !assert.NoError(f.t, json.NewDecoder(r.Body).Decode(&body)) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	received := fakeRequest{
		method:        r.Method,
		path:          r.URL.Path,
		contentType:   r.Header.Get("Content-Type"),
		authorization: r.Header.Get("Authorization"),
		imagesHeader:  r.Header.Get("TypeSafe-Danger-Zone-Images"),
		body:          body,
	}

	f.lock.Lock()
	reply, replied := f.replies[len(f.requests)]
	f.requests = append(f.requests, received)
	f.inFlight++
	f.maxSeen = max(f.maxSeen, f.inFlight)
	var gathered chan struct{}
	if f.together > 0 {
		if f.gathering == nil {
			f.gathering = make(chan struct{})
		}
		gathered = f.gathering
		f.waiting++
		if f.waiting == f.together {
			close(f.gathering)
			f.gathering, f.waiting = nil, 0
		}
	}
	f.lock.Unlock()
	defer func() {
		f.lock.Lock()
		f.inFlight--
		f.lock.Unlock()
	}()

	time.Sleep(f.delay)
	if gathered != nil {
		select {
		case <-gathered:
		case <-r.Context().Done():
		}
	}
	if f.release != nil {
		<-f.release
	}

	switch {
	case replied && reply.hangUp:
		conn, _, err := http.NewResponseController(w).Hijack()
		if assert.NoError(f.t, err) {
			conn.Close()
		}
	case replied:
		w.WriteHeader(reply.status)
		io.WriteString(w, reply.body)
	case f.tooLarge != nil && f.tooLarge(received.documents()):
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"error_type":"max_tokens_exceeded"}`)
	default:
		json.NewEncoder(w).Encode(map[string]any{
			"model":   body.Model,
			"answers": f.answers(body),
			"usage":   map[string]int{"input_tokens": 100, "output_tokens": 10},
		})
	}
}

func (f *fakeTypeSafeAI) answers(body wireRequest) map[string]any {
	answer := func(value any) map[string]any {
		document := value.(string)
		if asset, ok := body.Assets[document]; ok {
			document = asset.DataBase64
		}
		probability, ok := f.probabilities[document]
		if !ok {
			probability = 0.5
		}
		return map[string]any{"type": "noul", "noul": probability}
	}
	if state, ok := body.State.(map[string]any); ok {
		answers := make(map[string]any, len(state))
		for key, value := range state {
			assert.True(f.t, strings.HasPrefix(body.Questions[key].Instructions, "In "+key+": "),
				"the question of %s names it", key)
			answers[key] = answer(value)
		}
		return answers
	}
	return map[string]any{"q": answer(body.State)}
}

func classConfig(baseURL string, settings map[string]any) moduletools.ClassConfig {
	classSettings := map[string]any{"baseURL": baseURL}
	for name, value := range settings {
		classSettings[name] = value
	}
	return fakeClassConfig{settings: classSettings}
}

type fakeClassConfig struct {
	settings map[string]any
}

func (f fakeClassConfig) Class() map[string]any {
	return f.settings
}

func (f fakeClassConfig) ClassByModuleName(string) map[string]any {
	return f.settings
}

func (f fakeClassConfig) PropertiesDataTypes() map[string]schema.DataType {
	return map[string]schema.DataType{
		"text":   schema.DataTypeText,
		"photo":  schema.DataTypeBlob,
		"tags":   schema.DataTypeTextArray,
		"legacy": schema.DataTypeString,
	}
}

func (f fakeClassConfig) Tenant() string                 { return "" }
func (f fakeClassConfig) TargetVector() string           { return "" }
func (f fakeClassConfig) Property(string) map[string]any { return nil }
func (f fakeClassConfig) Config() *config.Config         { return nil }

func TestRankReturnsADeadlineErrorWhenTheProviderDoesNotAnswer(t *testing.T) {
	f, release := newBlockingFake(t)
	defer release()
	c := New(defaultFakeKey, 50*time.Millisecond, nullLogger())

	// The context deadline only stops a client without a timeout; the
	// assertion on the elapsed time then fails.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	_, err := c.Rank(ctx, churn, "text", []string{noRefund}, classConfig(f.url(), nil))

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestRankReadsTheHeadersFromGRPCMetadata(t *testing.T) {
	classServer, headerServer := newFake(t), newFake(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-typesafeai-api-key", "metadataKey",
		"x-typesafeai-baseurl", headerServer.url(),
	))

	_, err := newTestClient("envKey").Rank(ctx, churn, "text", []string{noRefund}, classConfig(classServer.url(), nil))

	require.NoError(t, err)
	assert.Empty(t, classServer.received())
	requests := headerServer.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "Bearer metadataKey", requests[0].authorization)
}
