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
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(noRefund), classConfig(f.url(), nil))

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

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(documents...), classConfig(f.url(), map[string]any{"model": "jev-test"}))

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

func TestRankBatchesByCountAndCharacters(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		documents []string
		wantSizes []int
	}{
		{
			name:      "120 short documents",
			documents: numbered("ticket %d", 120),
			wantSizes: []int{20, 50, 50},
		},
		{
			name: "3 documents of 40,000 characters",
			documents: []string{
				strings.Repeat("a", 40_000), strings.Repeat("b", 40_000), strings.Repeat("c", 40_000),
			},
			wantSizes: []int{1, 2},
		},
		{
			// A document costs 1 byte, its question prefix 16 and the query
			// 20,000: 20,017 bytes, so 4 fit in a request.
			name:      "12 one-byte documents with a 20,000-byte query",
			query:     strings.Repeat("q", 20_000),
			documents: strings.Split("abcdefghijkl", ""),
			wantSizes: []int{4, 4, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.probabilities = distinctProbabilities(tt.documents)
			query := tt.query
			if query == "" {
				query = churn
			}

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), query, "text",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.NoError(t, err)
			assert.Equal(t, expectedScores(tt.documents, f.probabilities), res.DocumentScores)
			assert.Equal(t, tt.wantSizes, sizes(f.received()))
		})
	}
}

func TestRankRejectsAQueryTooLongToRepeatInARequest(t *testing.T) {
	longest := MaxBatchCharacters - len("In document_49: ")
	tests := []struct {
		name      string
		query     string
		property  string
		documents []string
		wantErr   string
	}{
		{
			name:      "a query whose question fills a request",
			query:     strings.Repeat("q", longest),
			property:  "text",
			documents: []string{noRefund},
			wantErr:   "rerank.query is 99984 bytes; reranker-typesafeai repeats it for every document within 100000 bytes per request",
		},
		{
			name:      "the same query before an image outside the limits",
			query:     strings.Repeat("q", longest),
			property:  "photo",
			documents: []string{encodedImage(t, "png", 100, 100)},
			wantErr:   "rerank.query is 99984 bytes; reranker-typesafeai repeats it for every document within 100000 bytes per request",
		},
		{
			name:      "one byte shorter",
			query:     strings.Repeat("q", longest-1),
			property:  "text",
			documents: []string{noRefund},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), tt.query, tt.property,
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Empty(t, f.received())
				return
			}
			require.NoError(t, err)
			assert.Len(t, res.DocumentScores, 1)
			assert.Len(t, f.received(), 1)
		})
	}
}

func TestRankHalvesARequestTheAPIRejectsForItsSize(t *testing.T) {
	t.Run("five documents", func(t *testing.T) {
		f := newFake(t)
		f.tooLarge = func(documents int) bool { return documents > 2 }
		documents := []string{noRefund, chargedTwice, changeEmail, "Cancel my plan today.", "Thanks, all good now."}
		f.probabilities = distinctProbabilities(documents)
		logger, hook := test.NewNullLogger()
		logger.SetLevel(logrus.DebugLevel)
		c := New(defaultFakeKey, testTimeout, DefaultMaxConcurrentRequests, logger)

		res, err := c.RankProperty(context.Background(), churn, "text", documentsOf(documents...), classConfig(f.url(), nil))

		require.NoError(t, err)
		assert.Equal(t, expectedScores(documents, f.probabilities), res.DocumentScores)
		// 5 is rejected, then 2 and 3; 3 is rejected, then 1 and 2.
		assert.Equal(t, []int{1, 2, 2, 3, 5}, sizes(f.received()))
		assert.Equal(t, logrus.Fields{"documents": 5, "judged": 5, "requests": int64(5), "input_tokens": int64(300)},
			rankLogFields(t, hook))
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

			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.EqualError(t, err, "object 00000000-0000-0000-0000-000000000000: "+
				`connection to TypeSafe AI API failed with status 400: {"error_type":"max_tokens_exceeded"}`)
			assert.Len(t, f.received(), tt.wantRequests)
		})
	}
}

func TestRankSkipsEmptyAndDuplicateDocuments(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"a": 0.8, "b": 0.2}

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf("", "a", "a", "b"), classConfig(f.url(), nil))

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

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf("", ""), classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: "", Score: 0}, {Document: "", Score: 0}}, res.DocumentScores)
	assert.Empty(t, f.received())
}

func TestRankRequiresAQuery(t *testing.T) {
	f := newFake(t)

	_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), "", "text",
		documentsOf(noRefund), classConfig(f.url(), nil))

	require.EqualError(t, err, "rerank.query is required by reranker-typesafeai: the query is the condition the model judges")
	assert.Empty(t, f.received())
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
			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, tt.property,
				documentsOf(noRefund), tt.cfg)

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

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "legacy",
		documentsOf(noRefund), classConfig(f.url(), nil))

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
			name:      "400 to a single document names its object",
			documents: []string{noRefund},
			status:    http.StatusBadRequest,
			body:      `{"error":"unsupported content"}`,
			wantErr: "object 00000000-0000-0000-0000-000000000000: " +
				`connection to TypeSafe AI API failed with status 400: {"error":"unsupported content"}`,
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

			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, f.received(), 1)
		})
	}
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
			name:          "base URL header without a key header",
			serverKey:     "env",
			headerBaseURL: true,
			wantErr:       "X-Typesafeai-Baseurl needs X-Typesafeai-Api-Key: the server's key is not sent to a host named by the request",
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

			_, err := newTestClient(tt.serverKey).RankProperty(ctx, churn, "text",
				documentsOf(noRefund), classConfig(classServer.url(), nil))

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

func TestRankKeepsTheBaseURLPath(t *testing.T) {
	f := newFake(t)

	_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(noRefund), classConfig(f.url()+"/typesafe", nil))

	require.NoError(t, err)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "/typesafe/v1/systemone", requests[0].path)
}

func TestRankCapsRequestsInFlight(t *testing.T) {
	f := newFake(t)
	f.delay = 20 * time.Millisecond
	c := New(defaultFakeKey, testTimeout, 2, nullLogger())
	// 32 documents of 3,000 characters and their questions fit in a request,
	// so 200 are 7 requests.
	queries := [][]string{
		numbered("first %03d "+strings.Repeat("x", 2_990), 200),
		numbered("second %03d "+strings.Repeat("x", 2_989), 200),
	}

	var wg sync.WaitGroup
	errs := make([]error, len(queries))
	for i, documents := range queries {
		require.Equal(t, 3_000, len(documents[0]))
		wg.Go(func() {
			_, errs[i] = c.RankProperty(context.Background(), churn, "text", documentsOf(documents...), classConfig(f.url(), nil))
		})
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	assert.Len(t, f.received(), 14)
	assert.Equal(t, 2, f.maxInFlight())
}

func TestRankStopsOnContextCancel(t *testing.T) {
	const inFlight = 4
	// 500 short documents are ten requests.
	documents := numbered("ticket %d", 500)

	t.Run("with requests in flight", func(t *testing.T) {
		f, release := newBlockingFake(t)
		defer release()
		c := New(defaultFakeKey, testTimeout, inFlight, nullLogger())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		result := rankInBackground(t, ctx, c, f, documents)
		require.Eventually(t, func() bool { return len(f.received()) == inFlight }, 5*time.Second, time.Millisecond)

		cancel()
		requireCanceledWithinASecond(t, result)
		assert.Zero(t, len(c.inFlight), "every in-flight slot is released")

		release()
		waitFor(t, &f.handlers)
		assert.Len(t, f.received(), inFlight, "the requests waiting for a slot are not sent")
		res, err := c.RankProperty(context.Background(), churn, "text", documentsOf(noRefund), classConfig(f.url(), nil))
		require.NoError(t, err)
		assert.Len(t, res.DocumentScores, 1)
	})

	t.Run("while waiting for a slot another query holds", func(t *testing.T) {
		f, release := newBlockingFake(t)
		defer release()
		c := New(defaultFakeKey, testTimeout, inFlight, nullLogger())
		other := rankInBackground(t, context.Background(), c, f, documents)
		require.Eventually(t, func() bool { return len(f.received()) == inFlight }, 5*time.Second, time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		result := rankInBackground(t, ctx, c, f, []string{noRefund})
		cancel()
		requireCanceledWithinASecond(t, result)

		release()
		require.NoError(t, <-other)
		assert.Len(t, f.received(), 10, "the cancelled query sent nothing")
	})
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
		_, err := c.RankProperty(ctx, churn, "text", documentsOf(documents...), classConfig(f.url(), nil))
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
		t.Fatal("RankProperty did not return within a second of the cancel")
	}
}

func TestNewRaisesACapBelowOneToOne(t *testing.T) {
	tests := []struct {
		name                  string
		maxConcurrentRequests int
		wantCap               int
	}{
		{name: "zero", maxConcurrentRequests: 0, wantCap: 1},
		{name: "negative", maxConcurrentRequests: -3, wantCap: 1},
		{name: "one", maxConcurrentRequests: 1, wantCap: 1},
		{name: "the default", maxConcurrentRequests: DefaultMaxConcurrentRequests, wantCap: 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			c := New(defaultFakeKey, testTimeout, tt.maxConcurrentRequests, nullLogger())

			require.Equal(t, tt.wantCap, cap(c.inFlight))
			res, err := c.RankProperty(context.Background(), churn, "text", documentsOf(noRefund, chargedTwice),
				classConfig(f.url(), nil))
			require.NoError(t, err)
			assert.Len(t, res.DocumentScores, 2)
		})
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
			wantErr:   "parse TypeSafe AI response",
		},
		{
			name:      "a body above the limit",
			documents: []string{noRefund},
			body:      `{"answers":{"q":{"type":"noul","noul":0.5}}}` + strings.Repeat(" ", maxResponseBodyBytes),
			wantErr:   "TypeSafe AI response exceeds 1048576 bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Len(t, f.received(), 1)
		})
	}
}

func TestRankSendsABlobPropertyAsImages(t *testing.T) {
	const damaged = "The photo shows a damaged car"

	t.Run("two images share a request", func(t *testing.T) {
		images := []string{encodedImage(t, "png", 300, 300), encodedImage(t, "png", 320, 300)}
		f := newFake(t)
		f.probabilities = map[string]float64{images[0]: 0.9, images[1]: 0.2}

		res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), damaged, "photo",
			documentsOf(images...), classConfig(f.url(), nil))

		require.NoError(t, err)
		assert.Equal(t, expectedScores(images, f.probabilities), res.DocumentScores)
		requests := f.received()
		require.Len(t, requests, 1)
		assert.Equal(t, "true", requests[0].imagesHeader)
		body := requests[0].body
		assert.Equal(t, map[string]any{"document_0": "image_0", "document_1": "image_1"}, body.State)
		assert.Equal(t, map[string]wireAsset{
			"image_0": {Type: "image", DataBase64: images[0], Format: "png"},
			"image_1": {Type: "image", DataBase64: images[1], Format: "png"},
		}, body.Assets)
		assert.Equal(t, wireQuestion{Type: "noul", Instructions: "In document_1: " + damaged}, body.Questions["document_1"])
	})

	t.Run("five images are two requests of 4 and 1", func(t *testing.T) {
		images := make([]string, 5)
		for i := range images {
			images[i] = encodedImage(t, "png", 300+i, 300)
		}
		f := newFake(t)
		f.probabilities = distinctProbabilities(images)

		res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), damaged, "photo",
			documentsOf(images...), classConfig(f.url(), nil))

		require.NoError(t, err)
		assert.Equal(t, expectedScores(images, f.probabilities), res.DocumentScores)
		requests := f.received()
		assert.Equal(t, []int{1, 4}, sizes(requests))
		for _, r := range requests {
			assert.Equal(t, "true", r.imagesHeader)
			if r.documents() == 1 {
				assert.Equal(t, "image_0", r.body.State)
				assert.Equal(t, map[string]wireAsset{"image_0": {Type: "image", DataBase64: images[4], Format: "png"}}, r.body.Assets)
				assert.Equal(t, wireQuestion{Type: "noul", Instructions: damaged}, r.body.Questions["q"])
			}
		}
	})

	t.Run("a pair the API rejects for its size is sent as two requests", func(t *testing.T) {
		images := []string{encodedImage(t, "jpeg", 300, 300), encodedImage(t, "png", 300, 300)}
		f := newFake(t)
		f.tooLarge = func(documents int) bool { return documents > 1 }
		f.probabilities = map[string]float64{images[0]: 0.7, images[1]: 0.1}

		res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), damaged, "photo",
			documentsOf(images...), classConfig(f.url(), nil))

		require.NoError(t, err)
		assert.Equal(t, expectedScores(images, f.probabilities), res.DocumentScores)
		requests := f.received()
		assert.Equal(t, []int{1, 1, 2}, sizes(requests))
		for _, r := range requests {
			assert.Equal(t, "true", r.imagesHeader)
		}
		assert.Equal(t, "image_0", requests[1].body.State)
		assert.Equal(t, wireAsset{Type: "image", DataBase64: images[0], Format: "jpeg"}, requests[1].body.Assets["image_0"])
		assert.Equal(t, "image_0", requests[2].body.State)
		assert.Equal(t, wireAsset{Type: "image", DataBase64: images[1], Format: "png"}, requests[2].body.Assets["image_0"])
	})
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
			wantErr:   "object 00000000-0000-0000-0000-000000000001: image: 100x100 pixels: the API takes 280 to 1568 pixels per side",
		},
		{
			name:      "after an empty document",
			documents: []string{valid, "", small},
			wantErr:   "object 00000000-0000-0000-0000-000000000002: image: 100x100 pixels: the API takes 280 to 1568 pixels per side",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), "The photo shows a damaged car", "photo",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
		})
	}
}

func TestRankSendsBase64TextAsText(t *testing.T) {
	encoded := encodedImage(t, "png", 300, 300)
	f := newFake(t)
	f.probabilities = map[string]float64{encoded: 0.4}

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(encoded), classConfig(f.url(), nil))

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
	return New(apiKey, testTimeout, DefaultMaxConcurrentRequests, nullLogger())
}

// documentsOf gives the value at position i the ID
// 00000000-0000-0000-0000-<i in 12 digits>.
func documentsOf(values ...string) []ent.Document {
	out := make([]ent.Document, len(values))
	for i, value := range values {
		out[i] = ent.Document{ID: strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)), Value: value}
	}
	return out
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

func rankLogFields(t *testing.T, hook *test.Hook) logrus.Fields {
	t.Helper()
	var found []logrus.Fields
	for _, entry := range hook.AllEntries() {
		if _, ok := entry.Data["judged"]; ok {
			found = append(found, entry.Data)
		}
	}
	require.Len(t, found, 1, "one log line per call")
	return found[0]
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
	// release, when set, holds every handler until it is closed.
	release  chan struct{}
	handlers sync.WaitGroup

	lock     sync.Mutex
	requests []fakeRequest
	inFlight int
	maxSeen  int
}

func newFake(t *testing.T) *fakeTypeSafeAI {
	f := &fakeTypeSafeAI{t: t}
	f.server = httptest.NewServer(f)
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
	f.lock.Unlock()
	defer func() {
		f.lock.Lock()
		f.inFlight--
		f.lock.Unlock()
	}()

	time.Sleep(f.delay)
	if f.release != nil {
		<-f.release
	}

	switch {
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
