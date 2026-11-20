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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

func TestRankOneRequestPerDocument(t *testing.T) {
	tests := []struct {
		name      string
		settings  map[string]any
		wantModel string
	}{
		{name: "the default model", wantModel: "gpt-6-luna"},
		{name: "the class model", settings: map[string]any{"model": "gpt-7-luna"}, wantModel: "gpt-7-luna"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.probabilities = map[string]float64{noRefund: 0.91, chargedTwice: 0.64, changeEmail: 0.03}
			documents := []string{noRefund, chargedTwice, changeEmail}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				documents, classConfig(f.url(), tt.settings))

			require.NoError(t, err)
			assert.Equal(t, &ent.RankResult{Query: churn, DocumentScores: []ent.DocumentScore{
				{Document: noRefund, Score: 0.91},
				{Document: chargedTwice, Score: 0.64},
				{Document: changeEmail, Score: 0.03},
			}}, res)
			requests := f.received()
			assert.ElementsMatch(t, documents, inputs(requests))
			for _, r := range requests {
				assert.Equal(t, http.MethodPost, r.method)
				assert.Equal(t, "/v1/decisions", r.path)
				assert.Equal(t, "application/json", r.contentType)
				assert.Equal(t, "Bearer apiKey", r.authorization)
				assert.JSONEq(t, decisionBody(t, tt.wantModel, r.input(), churn), r.rawBody)
			}
		})
	}
}

func TestRankScoresARefusedDocumentZero(t *testing.T) {
	tests := []struct {
		name       string
		refusals   map[string]bool
		wantScores []float64
	}{
		{
			name:       "one refused document among three",
			refusals:   map[string]bool{chargedTwice: true},
			wantScores: []float64{0.91, 0, 0.03},
		},
		{
			name:       "every document refused",
			refusals:   map[string]bool{noRefund: true, chargedTwice: true, changeEmail: true},
			wantScores: []float64{0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.probabilities = map[string]float64{noRefund: 0.91, chargedTwice: 0.64, changeEmail: 0.03}
			f.refusals = tt.refusals
			documents := []string{noRefund, chargedTwice, changeEmail}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				documents, classConfig(f.url(), nil))

			require.NoError(t, err)
			scores := make([]float64, len(res.DocumentScores))
			for i, s := range res.DocumentScores {
				scores[i] = s.Score
			}
			assert.Equal(t, tt.wantScores, scores)
			assert.Len(t, f.received(), len(documents))
		})
	}
}

// The first query leaves one connection in the pool. The transport sends a
// request again only when its connection came from the pool.
func TestRankSendsARequestAgainWhenAPooledConnectionBreaks(t *testing.T) {
	f := newFake(t)
	f.replies = map[int]fakeReply{1: {hangUp: true}}
	c := newTestClient(defaultFakeKey)

	_, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))
	require.NoError(t, err)
	require.Equal(t, int64(1), f.opened.Load())
	res, err := c.Rank(context.Background(), churn, "text", []string{chargedTwice}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: chargedTwice, Score: 0.5}}, res.DocumentScores)
	assert.Equal(t, []string{noRefund, chargedTwice, chargedTwice}, inputs(f.received()))
	assert.Equal(t, int64(2), f.opened.Load())
}

func TestRankSendsARequestOnce(t *testing.T) {
	tests := []struct {
		name string
		// pooled sends a query before, so that the request goes out on a
		// connection from the pool.
		pooled  bool
		reply   fakeReply
		wantErr string
	}{
		{
			name:    "a 429 on a connection from the pool",
			pooled:  true,
			reply:   fakeReply{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`},
			wantErr: "connection to OpenAI API failed with status 429: slow down",
		},
		{
			name:    "a 500 on a connection from the pool",
			pooled:  true,
			reply:   fakeReply{status: http.StatusInternalServerError, body: `{"error":{"message":"The server had an error"}}`},
			wantErr: "connection to OpenAI API failed with status 500: The server had an error",
		},
		{
			name:    "a 503 on a connection from the pool",
			pooled:  true,
			reply:   fakeReply{status: http.StatusServiceUnavailable, body: `{"error":{"message":"overloaded"}}`},
			wantErr: "connection to OpenAI API failed with status 503: overloaded",
		},
		{
			name:    "a 429 on a new connection",
			reply:   fakeReply{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`},
			wantErr: "connection to OpenAI API failed with status 429: slow down",
		},
		{
			name:    "a new connection that breaks",
			reply:   fakeReply{hangUp: true},
			wantErr: "send OpenAI request: ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			c := newTestClient(defaultFakeKey)
			want := []string{chargedTwice}
			if tt.pooled {
				_, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))
				require.NoError(t, err)
				want = []string{noRefund, chargedTwice}
			}
			f.replies = map[int]fakeReply{len(want) - 1: tt.reply}

			_, err := c.Rank(context.Background(), churn, "text", []string{chargedTwice}, classConfig(f.url(), nil))

			require.ErrorContains(t, err, tt.wantErr)
			assert.Equal(t, want, inputs(f.received()))
			assert.Equal(t, int64(1), f.opened.Load())
		})
	}
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
	assert.ElementsMatch(t, []string{"a", "b"}, inputs(f.received()))
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
	assert.ElementsMatch(t, []string{"a", "b"}, inputs(f.received()))
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

			require.EqualError(t, err, "rerank.query is required by reranker-openai: the query is the condition the model judges")
			assert.Empty(t, f.received())
		})
	}
}

func TestRankLimitsTheQuerySize(t *testing.T) {
	tests := []struct {
		name         string
		queryBytes   int
		wantErr      string
		wantRequests int
	}{
		{name: "at the limit", queryBytes: maxQueryBytes, wantRequests: 1},
		{
			name:       "one byte over the limit",
			queryBytes: maxQueryBytes + 1,
			wantErr:    "rerank.query is 100001 bytes; reranker-openai sends it with every document and takes at most 100000 bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), strings.Repeat("q", tt.queryBytes), "text",
				[]string{noRefund}, classConfig(f.url(), nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Len(t, f.received(), tt.wantRequests)
		})
	}
}

func TestRankCapsTheDocumentsOfAQuery(t *testing.T) {
	tests := []struct {
		name      string
		cap       int
		property  string
		documents []string
		wantSent  int
		wantErr   string
	}{
		{
			name:      "at the cap",
			cap:       3,
			property:  "text",
			documents: []string{"a", "b", "c"},
			wantSent:  3,
		},
		{
			name:      "one above the cap",
			cap:       3,
			property:  "text",
			documents: []string{"a", "b", "c", "d"},
			wantErr:   "rerank of 4 objects: reranker-openai sends one request per object and takes at most 3 per query",
		},
		{
			name:      "blank and duplicate documents do not count",
			cap:       3,
			property:  "text",
			documents: []string{"a", "a", "", "  \n", "b", "c", "b", "c"},
			wantSent:  3,
		},
		{
			name:      "the cap is checked before the images",
			cap:       1,
			property:  "photo",
			documents: []string{pngBase64, "%%%%"},
			wantErr:   "rerank of 2 objects: reranker-openai sends one request per object and takes at most 1 per query",
		},
		{
			name:      "a cap below one is one",
			cap:       0,
			property:  "text",
			documents: []string{"a"},
			wantSent:  1,
		},
		{
			name:      "one above a cap below one",
			cap:       -2,
			property:  "text",
			documents: []string{"a", "b"},
			wantErr:   "rerank of 2 objects: reranker-openai sends one request per object and takes at most 1 per query",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			c := New(defaultFakeKey, "", testTimeout, tt.cap, nullLogger())

			res, err := c.Rank(context.Background(), churn, tt.property, tt.documents,
				classConfig(f.url(), nil))

			assert.Len(t, f.received(), tt.wantSent)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, res)
				return
			}
			require.NoError(t, err)
			assert.Len(t, res.DocumentScores, len(tt.documents))
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
			wantErr:  `property "tags" is not a text or blob property; reranker-openai reranks text and blob properties`,
		},
		{
			name:     "a property the class does not have",
			property: "missing",
			cfg:      classConfig(f.url(), nil),
			wantErr:  `property "missing" is not a text or blob property; reranker-openai reranks text and blob properties`,
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

func TestRankSendsABlobPropertyAsAnImage(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"data:image/png;base64," + pngBase64: 0.8, "data:image/jpeg;base64," + jpegBase64: 0.2}
	documents := []string{pngBase64, jpegBase64}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), "The product has visible damage", "photo",
		documents, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: pngBase64, Score: 0.8}, {Document: jpegBase64, Score: 0.2}}, res.DocumentScores)
	requests := f.received()
	require.Len(t, requests, 2)
	for _, r := range requests {
		assert.JSONEq(t, fmt.Sprintf(`{
			"model": "gpt-6-luna",
			"input": [{"role": "user", "content": [{"type": "input_image", "image_url": %q}]}],
			"questions": [{"type": "predicate", "name": "condition", "instructions": "The product has visible damage"}]
		}`, r.input()), r.rawBody)
	}
	assert.ElementsMatch(t, []string{"data:image/png;base64," + pngBase64, "data:image/jpeg;base64," + jpegBase64}, inputs(requests))
}

func TestRankRejectsAnImageBeforeSending(t *testing.T) {
	tests := []struct {
		name      string
		documents []string
		wantErr   string
	}{
		{
			name:      "base64 of text",
			documents: []string{pngBase64, textBase64, jpegBase64},
			wantErr:   "image: not a PNG, JPEG, GIF or WebP image",
		},
		{
			name:      "a BMP",
			documents: []string{pngBase64, bmpBase64},
			wantErr:   "image: not a PNG, JPEG, GIF or WebP image",
		},
		{
			name:      "not base64",
			documents: []string{pngBase64, "%%%%"},
			wantErr:   "image: not base64",
		},
		{
			name:      "20 MiB + 1 byte",
			documents: []string{pngBase64, pngOfSize(maxImageBytes + 1)},
			wantErr:   "image: 20971521 bytes exceed the limit of 20 MiB per image",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "photo",
				tt.documents, classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
		})
	}
}

func TestRankSendsBase64TextAsText(t *testing.T) {
	f := newFake(t)

	_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{pngBase64}, classConfig(f.url(), nil))

	require.NoError(t, err)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, pngBase64, requests[0].body.Input)
}

// Classes created before v1.19 still store the deprecated string type.
func TestRankReadsTheDeprecatedStringTypeAsText(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.9}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "legacy",
		[]string{noRefund}, classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{{Document: noRefund, Score: 0.9}}, res.DocumentScores)
	assert.Equal(t, []string{noRefund}, inputs(f.received()))
}

func TestRankFailsAtOnceOnProviderErrors(t *testing.T) {
	tests := []struct {
		name    string
		reply   fakeReply
		wantErr string
	}{
		{
			name:    "429",
			reply:   fakeReply{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`},
			wantErr: "connection to OpenAI API failed with status 429: slow down",
		},
		{
			name: "500",
			reply: fakeReply{
				status: http.StatusInternalServerError,
				body:   `{"error":{"message":"The server had an error","type":"server_error","code":null}}`,
			},
			wantErr: "connection to OpenAI API failed with status 500: The server had an error",
		},
		{
			name:    "a body without a message",
			reply:   fakeReply{status: http.StatusBadGateway, body: "<html>Bad Gateway</html>"},
			wantErr: "connection to OpenAI API failed with status 502",
		},
		{
			name:    "a message above 512 bytes is cut",
			reply:   fakeReply{status: http.StatusInternalServerError, body: `{"error":{"message":"` + strings.Repeat("x", 2000) + `"}}`},
			wantErr: "connection to OpenAI API failed with status 500: " + strings.Repeat("x", 512),
		},
		{
			name:    "the cut does not split a character",
			reply:   fakeReply{status: http.StatusInternalServerError, body: `{"error":{"message":"` + strings.Repeat("x", 511) + `é"}}`},
			wantErr: "connection to OpenAI API failed with status 500: " + strings.Repeat("x", 511),
		},
		{
			name:    "400 quotes the message",
			reply:   fakeReply{status: http.StatusBadRequest, body: `{"error":{"message":"Invalid input","type":"invalid_request_error"}}`},
			wantErr: "connection to OpenAI API failed with status 400: Invalid input",
		},
		{
			name: "400 with a param in the input",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Invalid value: 'input_image'.","type":"invalid_request_error","param":"input[0].type","code":"invalid_value"}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Invalid value: 'input_image'.",
		},
		{
			name: "400 with a null param",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Invalid input","type":"invalid_request_error","param":null,"code":null}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Invalid input",
		},
		{
			name: "400 with a number as the code keeps the message",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Invalid input","param":7,"code":400}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Invalid input",
		},
		{
			name: "400 with a message that is not a string",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":["Unsupported model"],"param":"model","code":"invalid_value"}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400",
		},
		{
			name: "400 with an error that is a string quotes it",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":"Invalid input"}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Invalid input",
		},
		{
			name: "400 with a param in the questions",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Invalid instructions","type":"invalid_request_error","param":"questions[0].instructions","code":"invalid_value"}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Invalid instructions",
		},
		{
			name: "400 with the param model",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Unsupported model","type":"invalid_request_error","param":"model","code":"invalid_value"}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Unsupported model",
		},
		{
			name: "400 with the code model_not_found",
			reply: fakeReply{
				status: http.StatusBadRequest,
				body:   `{"error":{"message":"Unknown model","type":"invalid_request_error","param":null,"code":"model_not_found"}}`,
			},
			wantErr: "connection to OpenAI API failed with status 400: Unknown model",
		},
		{
			// The API's reply to an unknown model and to a model of another kind.
			name: "404 for an unknown model",
			reply: fakeReply{
				status: http.StatusNotFound,
				body: "{\"error\":{\"message\":\"The model `x` does not exist or you do not have access to it.\"," +
					"\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"model_not_found\"}}",
			},
			wantErr: "connection to OpenAI API failed with status 404: The model `x` does not exist or you do not have access to it.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: tt.reply}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				[]string{"", noRefund}, classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Len(t, f.received(), 1)
		})
	}
}

// 33 documents are 33 requests, one more than a query sends at once. The fake
// answers 429 once the first 32 have arrived, and the first failure cancels
// the query before it frees a slot, so the 33rd is never sent.
func TestRankStopsAfterTheFirstProviderError(t *testing.T) {
	f := newFake(t)
	f.together = maxConcurrentRequests
	f.replies = map[int]fakeReply{}
	for i := range maxConcurrentRequests + 1 {
		f.replies[i] = fakeReply{status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`}
	}

	res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		numbered("ticket %d", maxConcurrentRequests+1), classConfig(f.url(), nil))

	require.EqualError(t, err, "connection to OpenAI API failed with status 429: slow down")
	assert.Nil(t, res)
	assert.Len(t, f.received(), maxConcurrentRequests)
}

func TestRankSendsNothingForAClassBaseURLWithoutAScheme(t *testing.T) {
	_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
		[]string{noRefund}, classConfig("api.openai.com", nil))

	require.EqualError(t, err, `send OpenAI request: Post "api.openai.com/v1/decisions": unsupported protocol scheme ""`)
}

func TestRankReportsA401AndA403WithoutTheBody(t *testing.T) {
	const (
		secret      = "sk-secret"
		rejectedKey = `{"error":{"message":"Incorrect API key provided: sk-secret","code":"invalid_api_key"}}`
	)
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr string
	}{
		{name: "401", status: http.StatusUnauthorized, body: rejectedKey, wantErr: "OpenAI rejected the API key (status 401)"},
		{name: "403 about the key", status: http.StatusForbidden, body: rejectedKey, wantErr: "OpenAI denied the request (status 403)"},
		{
			name:    "403 about the model",
			status:  http.StatusForbidden,
			body:    `{"error":{"message":"Project proj_1 of sk-secret does not have access to model gpt-6-luna","code":"model_not_found"}}`,
			wantErr: "OpenAI denied the request (status 403)",
		},
		{
			name:    "403 from a gateway",
			status:  http.StatusForbidden,
			body:    "<html>Forbidden for sk-secret</html>",
			wantErr: "OpenAI denied the request (status 403)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: tt.status, body: tt.body}}
			logger, hook := debugLogger()
			c := New(secret, "", testTimeout, MaxDocumentsPerQuery, logger)

			_, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, f.received(), 1)
			for _, entry := range hook.AllEntries() {
				line, err := entry.String()
				require.NoError(t, err)
				assert.NotContains(t, line, secret)
			}
		})
	}
}

// An OpenAI-compatible server can answer 200 with an error object.
func TestRankReportsAnErrorObjectInA200(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantErr   string
		wantScore float64
	}{
		{
			name:    "an error object",
			body:    `{"error":{"message":"The model is overloaded","type":"server_error"}}`,
			wantErr: "OpenAI API returned an error: The model is overloaded",
		},
		{
			name:    "an error that is a string",
			body:    `{"error":"The model is overloaded"}`,
			wantErr: "OpenAI API returned an error: The model is overloaded",
		},
		{
			name:    "an error object next to an answer",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":0.5}],"error":{"message":"The model is overloaded"}}`,
			wantErr: "OpenAI API returned an error: The model is overloaded",
		},
		{
			name:    "a message above 512 bytes is cut",
			body:    `{"error":{"message":"` + strings.Repeat("x", 2000) + `"}}`,
			wantErr: "OpenAI API returned an error: " + strings.Repeat("x", 512),
		},
		{
			name:    "an error object without a message",
			body:    `{"error":{"type":"server_error"}}`,
			wantErr: "OpenAI API returned an error",
		},
		{
			name:      "a null error",
			body:      `{"answers":[{"type":"predicate","name":"condition","probability":0.25}],"error":null}`,
			wantScore: 0.25,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				[]string{noRefund}, classConfig(f.url(), nil))

			assert.Len(t, f.received(), 1)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []ent.DocumentScore{{Document: noRefund, Score: tt.wantScore}}, res.DocumentScores)
		})
	}
}

func TestRankHeaders(t *testing.T) {
	tests := []struct {
		name               string
		serverKey          string
		serverOrganization string
		headerKey          string
		headerOrganization string
		headerBaseURL      bool
		wantErr            string
		wantAuth           string
		wantOrganization   []string
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
			name:          "base URL header without a key header",
			serverKey:     "env",
			headerBaseURL: true,
			wantErr:       "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:          "base URL header with a key header",
			serverKey:     "env",
			headerKey:     "headerKey",
			headerBaseURL: true,
			wantAuth:      "Bearer headerKey",
		},
		{
			name:               "organization from the environment",
			serverKey:          "env",
			serverOrganization: "org-env",
			wantAuth:           "Bearer env",
			wantOrganization:   []string{"org-env"},
		},
		{
			name:               "organization header wins",
			serverKey:          "env",
			serverOrganization: "org-env",
			headerOrganization: "org-header",
			wantAuth:           "Bearer env",
			wantOrganization:   []string{"org-header"},
		},
		{
			name:               "organization header without a server organization",
			serverKey:          "env",
			headerOrganization: "org-header",
			wantAuth:           "Bearer env",
			wantOrganization:   []string{"org-header"},
		},
		{
			name:               "server organization not sent to a host the request names",
			serverKey:          "env",
			serverOrganization: "org-env",
			headerKey:          "headerKey",
			headerBaseURL:      true,
			wantAuth:           "Bearer headerKey",
		},
		{
			name:               "organization header sent to a host the request names",
			serverKey:          "env",
			serverOrganization: "org-env",
			headerKey:          "headerKey",
			headerOrganization: "org-header",
			headerBaseURL:      true,
			wantAuth:           "Bearer headerKey",
			wantOrganization:   []string{"org-header"},
		},
		{
			name: "no key anywhere",
			wantErr: "OpenAI API key: no api key found neither in request header: X-Openai-Api-Key " +
				"nor in environment variable under OPENAI_APIKEY",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			ctx := context.Background()
			if tt.headerKey != "" {
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{tt.headerKey})
			}
			if tt.headerOrganization != "" {
				ctx = context.WithValue(ctx, "X-Openai-Organization", []string{tt.headerOrganization})
			}
			if tt.headerBaseURL {
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url()})
			}
			c := New(tt.serverKey, tt.serverOrganization, testTimeout, MaxDocumentsPerQuery, nullLogger())

			_, err := c.Rank(ctx, churn, "text", []string{noRefund}, classConfig(classServer.url(), nil))

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
			assert.Equal(t, tt.wantOrganization, requests[0].organization)
		})
	}
}

// A header base URL is a host the request names, even when it equals the class
// base URL, so it needs a key header.
func TestRankTakesAHeaderBaseURLAsAHostTheRequestNames(t *testing.T) {
	tests := []struct {
		name string
		// <class> stands for the URL in the class config.
		headerBaseURL string
	}{
		{name: "the class base URL", headerBaseURL: "<class>"},
		{name: "a path under the class base URL", headerBaseURL: "<class>/proxy"},
		{name: "another name of the class host", headerBaseURL: "http://localhost:<port>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			_, port, err := net.SplitHostPort(strings.TrimPrefix(f.url(), "http://"))
			require.NoError(t, err)
			headerBaseURL := strings.NewReplacer("<class>", f.url(), "<port>", port).Replace(tt.headerBaseURL)
			ctx := context.WithValue(context.Background(), "X-Openai-Baseurl", []string{headerBaseURL})

			_, err = New("env", "org-env", testTimeout, MaxDocumentsPerQuery, nullLogger()).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(f.url(), nil))

			require.EqualError(t, err, "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request")
			assert.Empty(t, f.received())
		})
	}
}

func TestRankRejectsABaseURLHeaderBeforeSending(t *testing.T) {
	tests := []struct {
		name string
		// <fake> stands for the fake's URL.
		baseURL  string
		validate bool
		wantErr  string
	}{
		{
			name:    "no scheme",
			baseURL: "api.openai.com",
			wantErr: "send OpenAI request: no HTTP response from the host named by X-Openai-Baseurl",
		},
		{
			name:     "http while MODULES_VALIDATE_BASE_URL is on",
			baseURL:  "<fake>/v1",
			validate: true,
			wantErr:  "baseURL must use HTTPS",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validate {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			f, classHost := newFake(t), newFake(t)
			ctx := context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"headerKey"})
			ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{strings.ReplaceAll(tt.baseURL, "<fake>", f.url())})

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classHost.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
			assert.Empty(t, classHost.received())
		})
	}
}

func TestRankKeepsTheBaseURLPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		fromHeader bool
		wantPath   string
	}{
		{name: "class base URL without a path", path: "", wantPath: "/v1/decisions"},
		{name: "class base URL with a gateway path", path: "/gateway", wantPath: "/gateway/v1/decisions"},
		{name: "header base URL with a gateway path", path: "/gateway", fromHeader: true, wantPath: "/gateway/v1/decisions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			classBaseURL := classServer.url() + tt.path
			used, unused := classServer, headerServer
			ctx := context.Background()
			if tt.fromHeader {
				classBaseURL = classServer.url()
				used, unused = headerServer, classServer
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url() + tt.path})
			}

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classBaseURL, nil))

			require.NoError(t, err)
			assert.Empty(t, unused.received())
			requests := used.received()
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantPath, requests[0].path)
		})
	}
}

// unexpectedHeaderHostReply is the error of a 200 reply the client cannot use
// from a host the request header named.
const unexpectedHeaderHostReply = "unexpected OpenAI response from the host named by X-Openai-Baseurl"

// A host named in the base URL header is untrusted: status errors quote no reply body of it.
func TestRankQuotesNoBodyOfAHostTheRequestNames(t *testing.T) {
	const banner = `{"error":{"message":"internal banner"}}`
	tests := []struct {
		name       string
		reply      fakeReply
		fromHeader bool
		wantErr    string
	}{
		{
			name:    "a 400 from the class base URL",
			reply:   fakeReply{status: http.StatusBadRequest, body: banner},
			wantErr: "connection to OpenAI API failed with status 400: internal banner",
		},
		{
			name:       "a 400 from the header base URL",
			reply:      fakeReply{status: http.StatusBadRequest, body: banner},
			fromHeader: true,
			wantErr:    "connection to OpenAI API failed with status 400",
		},
		{
			name:       "a 500 from the header base URL",
			reply:      fakeReply{status: http.StatusInternalServerError, body: banner},
			fromHeader: true,
			wantErr:    "connection to OpenAI API failed with status 500",
		},
		{
			name:       "a 401 from the header base URL",
			reply:      fakeReply{status: http.StatusUnauthorized, body: banner},
			fromHeader: true,
			wantErr:    "OpenAI rejected the API key (status 401)",
		},
		{
			name:    "a 200 with an error object from the class base URL",
			reply:   fakeReply{status: http.StatusOK, body: banner},
			wantErr: "OpenAI API returned an error: internal banner",
		},
		{
			name:       "a 200 with an error object from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: banner},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:    "a 200 above 1 MiB from the class base URL",
			reply:   fakeReply{status: http.StatusOK, body: strings.Repeat(" ", maxResponseBodyBytes+1)},
			wantErr: "OpenAI response exceeds 1048576 bytes",
		},
		{
			name:       "a 200 above 1 MiB from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: strings.Repeat(" ", maxResponseBodyBytes+1)},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:    "a 200 that is not JSON from the class base URL",
			reply:   fakeReply{status: http.StatusOK, body: "not json"},
			wantErr: "parse OpenAI response: invalid character 'o' in literal null (expecting 'u'): not json",
		},
		{
			name:       "a 200 that is not JSON from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: "not json"},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:       "a wrong answer name from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: `{"answers":[{"type":"predicate","name":"internal banner","probability":0.5}]}`},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:       "an answer of another type from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: `{"answers":[{"type":"internal banner","name":"condition","probability":0.5}]}`},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:       "a probability outside 0 to 1 from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: `{"answers":[{"type":"predicate","name":"condition","probability":7.25}]}`},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
		{
			name:       "two answers from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: `{"answers":[{"type":"predicate","name":"condition","probability":0.5},{"type":"predicate","name":"condition","probability":0.5}]}`},
			fromHeader: true,
			wantErr:    unexpectedHeaderHostReply,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			used, unused := classServer, headerServer
			ctx := context.Background()
			if tt.fromHeader {
				used, unused = headerServer, classServer
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url()})
			}
			used.replies = map[int]fakeReply{0: tt.reply}

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text",
				[]string{noRefund}, classConfig(classServer.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, used.received(), 1)
			assert.Empty(t, unused.received())
		})
	}
}

// The client timeout is 30 seconds, so a call that waited for the body fails
// the one-second bound.
func TestRankDoesNotReadAnErrorBodyItDoesNotQuote(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		fromHeader bool
		wantErr    string
	}{
		{
			name:       "a 500 from the header base URL",
			status:     http.StatusInternalServerError,
			fromHeader: true,
			wantErr:    "connection to OpenAI API failed with status 500",
		},
		{
			name:       "a 400 from the header base URL",
			status:     http.StatusBadRequest,
			fromHeader: true,
			wantErr:    "connection to OpenAI API failed with status 400",
		},
		{
			name:       "a 401 from the header base URL",
			status:     http.StatusUnauthorized,
			fromHeader: true,
			wantErr:    "OpenAI rejected the API key (status 401)",
		},
		{
			name:    "a 401 from the class base URL",
			status:  http.StatusUnauthorized,
			wantErr: "OpenAI rejected the API key (status 401)",
		},
		{
			name:    "a 403 from the class base URL",
			status:  http.StatusForbidden,
			wantErr: "OpenAI denied the request (status 403)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			used := classServer
			ctx := context.Background()
			if tt.fromHeader {
				used = headerServer
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url()})
			}
			used.replies = map[int]fakeReply{0: {status: tt.status, stalled: true}}

			result := rankInBackground(t, ctx, newTestClient(defaultFakeKey), classServer, []string{noRefund})

			select {
			case err := <-result:
				require.EqualError(t, err, tt.wantErr)
			case <-time.After(time.Second):
				used.server.CloseClientConnections()
				t.Fatal("Rank waited for the body of the error reply")
			}
			assert.Len(t, used.received(), 1)
		})
	}
}

// net/http quotes bytes of the host in its errors, and a dial error names the
// address that refused.
func TestRankHidesTheTransportErrorOfAHostTheRequestNames(t *testing.T) {
	const (
		banner = "SSH-2.0-OpenSSH_9.6"
		// net/http reads the trailer of a chunked body with the body.
		trailer = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{\r\n0\r\n" + banner + "\r\n\r\n"
	)
	tests := []struct {
		name       string
		host       func(t *testing.T) string
		fromHeader bool
		wantErr    string
		// <host> stands for the host and port.
		wantInErr []string
	}{
		{
			name:       "a banner that is not HTTP from the header base URL",
			host:       func(t *testing.T) string { return newRawHost(t, banner+"\r\n") },
			fromHeader: true,
			wantErr:    "send OpenAI request: no HTTP response from the host named by X-Openai-Baseurl",
		},
		{
			name:       "a closed port at the header base URL",
			host:       closedPort,
			fromHeader: true,
			wantErr:    "send OpenAI request: no HTTP response from the host named by X-Openai-Baseurl",
		},
		{
			name:       "a 200 with a trailer that is not a header from the header base URL",
			host:       func(t *testing.T) string { return newRawHost(t, trailer) },
			fromHeader: true,
			wantErr:    "read OpenAI response: no complete body from the host named by X-Openai-Baseurl",
		},
		{
			name:      "a 200 with a trailer that is not a header from the class base URL",
			host:      func(t *testing.T) string { return newRawHost(t, trailer) },
			wantInErr: []string{"read OpenAI response: ", banner},
		},
		{
			name:      "a banner that is not HTTP from the class base URL",
			host:      func(t *testing.T) string { return newRawHost(t, banner+"\r\n") },
			wantInErr: []string{`send OpenAI request: Post "http://<host>/v1/decisions": `, `malformed HTTP response "` + banner + `"`},
		},
		{
			name:      "a closed port at the class base URL",
			host:      closedPort,
			wantInErr: []string{`send OpenAI request: Post "http://<host>/v1/decisions": dial tcp <host>: `, "connection refused"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host := tt.host(t)
			classBaseURL := "http://" + host
			ctx := context.Background()
			if tt.fromHeader {
				classBaseURL = newFake(t).url()
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{"http://" + host})
			}

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text", []string{noRefund},
				classConfig(classBaseURL, nil))

			require.Error(t, err)
			for _, part := range tt.wantInErr {
				assert.ErrorContains(t, err, strings.ReplaceAll(part, "<host>", host))
			}
			if tt.fromHeader {
				assert.EqualError(t, err, tt.wantErr)
				for _, hidden := range []string{banner, "connection refused", host} {
					assert.NotContains(t, err.Error(), hidden)
				}
			}
		})
	}
}

func TestRankKeepsTheEndOfTheContextOfAHostTheRequestNames(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		// stalledBody sends the 200 and its headers, and never the body.
		stalledBody bool
		cancel      bool
		wantIs      error
		wantErr     string
	}{
		{
			name:    "the client timeout",
			timeout: 50 * time.Millisecond,
			wantIs:  context.DeadlineExceeded,
			wantErr: "send OpenAI request: context deadline exceeded",
		},
		{
			name:        "the client timeout while the body is read",
			timeout:     500 * time.Millisecond,
			stalledBody: true,
			wantIs:      context.DeadlineExceeded,
			wantErr:     "read OpenAI response: context deadline exceeded",
		},
		{
			name:        "a query cancelled while the body is read",
			timeout:     testTimeout,
			stalledBody: true,
			cancel:      true,
			wantIs:      context.Canceled,
			wantErr:     "read OpenAI response: context canceled",
		},
		{
			name:    "a cancelled query",
			timeout: testTimeout,
			cancel:  true,
			wantIs:  context.Canceled,
			wantErr: "send OpenAI request: context canceled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerHost, release := newBlockingFake(t)
			defer release()
			if tt.stalledBody {
				headerHost.replies = map[int]fakeReply{0: {status: http.StatusOK, stalled: true}}
				release()
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
			ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerHost.url()})
			c := New(defaultFakeKey, "", tt.timeout, MaxDocumentsPerQuery, nullLogger())
			headers := &headersSeen{base: c.httpClient.Transport, seen: make(chan struct{})}
			c.httpClient.Transport = headers

			result := rankInBackground(t, ctx, c, newFake(t), []string{noRefund})
			require.Eventually(t, func() bool { return len(headerHost.received()) == 1 }, 5*time.Second, time.Millisecond)
			if tt.stalledBody {
				select {
				case <-headers.seen:
				case <-time.After(5 * time.Second):
					t.Fatal("the client did not get the headers of the reply within 5 seconds")
				}
			}
			if tt.cancel {
				cancel()
			}

			select {
			case err := <-result:
				require.ErrorIs(t, err, tt.wantIs)
				assert.EqualError(t, err, tt.wantErr)
			case <-time.After(5 * time.Second):
				t.Fatal("Rank did not return within 5 seconds")
			}
		})
	}
}

// headersSeen closes seen once the client has the status and the headers of a
// reply, so a test can end the context while the body is read.
type headersSeen struct {
	base http.RoundTripper
	seen chan struct{}
}

func (h *headersSeen) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := h.base.RoundTrip(req)
	if err == nil {
		close(h.seen)
	}
	return res, err
}

// newRawHost answers every request with reply and closes the connection. It
// returns the host and port.
func newRawHost(t *testing.T, reply string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var serving sync.WaitGroup
	serving.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// A close with unread bytes resets the connection, and the client
			// could see the reset before the reply.
			if request, err := http.ReadRequest(bufio.NewReader(conn)); err == nil {
				io.Copy(io.Discard, request.Body)
				io.WriteString(conn, reply)
			}
			conn.Close()
		}
	})
	t.Cleanup(serving.Wait)
	t.Cleanup(func() { listener.Close() })
	return listener.Addr().String()
}

func closedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	return listener.Addr().String()
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "short", in: "a 400", want: "a 400"},
		{name: "at the limit", in: strings.Repeat("a", maxErrorMessageBytes), want: strings.Repeat("a", maxErrorMessageBytes)},
		{name: "above the limit", in: strings.Repeat("a", maxErrorMessageBytes+1), want: strings.Repeat("a", maxErrorMessageBytes)},
		{
			name: "the limit splits a character",
			in:   strings.Repeat("a", maxErrorMessageBytes-1) + "é",
			want: strings.Repeat("a", maxErrorMessageBytes-1),
		},
		{name: "a short text that ends inside a character", in: "caf\xc3", want: "caf"},
		{name: "a short text with bytes that are not UTF-8", in: "\xff\xfe not json", want: " not json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, truncate(tt.in))
		})
	}
}

func TestRankCutsLongErrorText(t *testing.T) {
	hugeNumber := `{"answers":[{"type":"predicate","name":"condition","probability":` + strings.Repeat("9", 100_000) + `}]}`
	tests := []struct {
		name       string
		fromHeader bool
	}{
		{name: "a decode error from the class base URL"},
		{name: "a decode error from the header base URL", fromHeader: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			used := classServer
			ctx := context.Background()
			if tt.fromHeader {
				used = headerServer
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url()})
			}
			used.replies = map[int]fakeReply{0: {status: http.StatusOK, body: hugeNumber}}

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text", []string{noRefund},
				classConfig(classServer.url(), nil))

			require.Error(t, err)
			if tt.fromHeader {
				assert.Equal(t, unexpectedHeaderHostReply, err.Error())
				return
			}
			assert.Contains(t, err.Error(), "parse OpenAI response: ")
			assert.LessOrEqual(t, len(err.Error()), 1200)
		})
	}
}

func TestRankReturnsADeadlineError(t *testing.T) {
	f, release := newBlockingFake(t)
	defer release()
	c := New(defaultFakeKey, "", 50*time.Millisecond, MaxDocumentsPerQuery, nullLogger())

	result := rankInBackground(t, context.Background(), c, f, []string{noRefund, chargedTwice, changeEmail})

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("Rank did not fail with the client timeout within 5 seconds")
	}
}

func TestRankReadsHeadersFromGRPCMetadata(t *testing.T) {
	classHost, headerHost := newFake(t), newFake(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-openai-api-key", "grpcKey",
		"x-openai-baseurl", headerHost.url(),
		"x-openai-organization", "grpcOrg",
	))

	_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text", []string{noRefund},
		classConfig(classHost.url(), nil))

	require.NoError(t, err)
	assert.Empty(t, classHost.received())
	requests := headerHost.received()
	require.Len(t, requests, 1)
	assert.Equal(t, "Bearer grpcKey", requests[0].authorization)
	assert.Equal(t, []string{"grpcOrg"}, requests[0].organization)
}

func TestRankKeepsTheNumberLiteralOutOfTheDecodeError(t *testing.T) {
	reply := func(probability string) string {
		return `{"answers":[{"type":"predicate","name":"condition","probability":` + probability + `}]}`
	}
	nines := strings.Repeat("9", 4096)
	tests := []struct {
		name       string
		fromHeader bool
		body       string
		notWant    string
		wantPrefix string
		maxLen     int
	}{
		{name: "4096 digits from the header host", fromHeader: true, body: reply(nines), notWant: "99", maxLen: 300},
		{name: "1e999 from the header host", fromHeader: true, body: reply("1e999"), notWant: "1e999", maxLen: 300},
		{
			name: "4096 digits from the class host", body: reply(nines), notWant: "99",
			wantPrefix: "parse OpenAI response: json: cannot unmarshal number into Go struct field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			classServer, headerServer := newFake(t), newFake(t)
			used := classServer
			ctx := context.Background()
			if tt.fromHeader {
				used = headerServer
				ctx = context.WithValue(ctx, "X-Openai-Api-Key", []string{"headerKey"})
				ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{headerServer.url()})
			}
			used.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			_, err := newTestClient(defaultFakeKey).Rank(ctx, churn, "text", []string{noRefund},
				classConfig(classServer.url(), nil))

			require.Error(t, err)
			message := err.Error()
			if tt.fromHeader {
				assert.Equal(t, unexpectedHeaderHostReply, message)
				return
			}
			require.Contains(t, message, "probability")
			if tt.wantPrefix != "" {
				require.True(t, strings.HasPrefix(message, tt.wantPrefix), message)
				// the body excerpt is appended after the decode error
				message = strings.TrimPrefix(message, tt.wantPrefix)
				message, _, _ = strings.Cut(message, ": {")
			}
			assert.NotContains(t, message, tt.notWant)
			if tt.maxLen > 0 {
				assert.Less(t, len(message), tt.maxLen)
			}
		})
	}
}

// A query sends at most 32 requests at once, and 4 when they carry images.
// The fake holds every request, so a query without the limit would send them
// all.
func TestRankCapsRequestsInFlightPerQuery(t *testing.T) {
	images := make([]string, 2*maxConcurrentImageRequests)
	for i := range images {
		images[i] = pngOfSize(100 + i)
	}
	tests := []struct {
		name      string
		property  string
		documents []string
		wantCap   int
	}{
		{name: "text", property: "text", documents: numbered("ticket %d", 2*maxConcurrentRequests), wantCap: maxConcurrentRequests},
		{name: "images", property: "photo", documents: images, wantCap: maxConcurrentImageRequests},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, release := newBlockingFake(t)
			defer release()
			result := make(chan error, 1)
			var call sync.WaitGroup
			defer call.Wait()
			call.Go(func() {
				_, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, tt.property,
					tt.documents, classConfig(f.url(), nil))
				result <- err
			})

			require.Eventually(t, func() bool { return len(f.received()) == tt.wantCap }, 5*time.Second, time.Millisecond)
			time.Sleep(50 * time.Millisecond)
			assert.Len(t, f.received(), tt.wantCap, "no request beyond the limit is sent while the others wait")

			release()
			require.NoError(t, <-result)
			assert.Len(t, f.received(), 2*tt.wantCap)
		})
	}
}

// Without the limit of the error group a query starts one goroutine per
// document.
func TestRankStartsNoMoreGoroutinesThanTheLimit(t *testing.T) {
	const slots, documents = maxConcurrentRequests, 200
	synctest.Test(t, func(t *testing.T) {
		c := newTestClient(defaultFakeKey)
		transport := &holdingTransport{release: make(chan struct{})}
		c.httpClient.Transport = transport
		var calls sync.WaitGroup
		var err error
		calls.Go(func() {
			_, err = c.Rank(context.Background(), churn, "text",
				numbered("ticket %d", documents), classConfig("http://unused.invalid", nil))
		})

		synctest.Wait()
		assert.Equal(t, slots, goroutinesIn("clients.(*client).judge("))
		assert.Equal(t, int64(slots), transport.count.Load())

		close(transport.release)
		calls.Wait()
		require.NoError(t, err)
		assert.Equal(t, int64(documents), transport.count.Load())
	})
}

// holdingTransport answers every request with a probability once release is
// closed.
type holdingTransport struct {
	release chan struct{}
	count   atomic.Int64
}

func (h *holdingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h.count.Add(1)
	select {
	case <-h.release:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	body := `{"answers":[{"type":"predicate","name":"condition","probability":0.5}]}`
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// goroutinesIn counts the goroutines with a frame of the function.
func goroutinesIn(function string) int {
	stacks := make([]byte, 1<<20)
	for {
		n := runtime.Stack(stacks, true)
		if n < len(stacks) {
			stacks = stacks[:n]
			break
		}
		stacks = make([]byte, 2*len(stacks))
	}
	return strings.Count(string(stacks), function)
}

func TestRankStopsOnContextCancel(t *testing.T) {
	// More documents than a query sends at once.
	documents := numbered("ticket %d", 2*maxConcurrentRequests)
	f, release := newBlockingFake(t)
	defer release()
	c := newTestClient(defaultFakeKey)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := rankInBackground(t, ctx, c, f, documents)
	require.Eventually(t, func() bool { return len(f.received()) == maxConcurrentRequests }, 5*time.Second, time.Millisecond)

	cancel()
	requireCanceledWithinASecond(t, result)

	release()
	waitFor(t, &f.handlers)
	assert.Len(t, f.received(), maxConcurrentRequests, "the requests waiting for a slot are not sent")
	res, err := c.Rank(context.Background(), churn, "text", []string{noRefund}, classConfig(f.url(), nil))
	require.NoError(t, err)
	assert.Len(t, res.DocumentScores, 1)
}

// Defer release: the calls' cleanups wait for the handlers.
func newBlockingFake(t *testing.T) (*fakeOpenAI, func()) {
	f := newFake(t)
	f.release = make(chan struct{})
	return f, sync.OnceFunc(func() { close(f.release) })
}

func rankInBackground(t *testing.T, ctx context.Context, c *client, f *fakeOpenAI, documents []string) <-chan error {
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
		body      string
		wantErr   string
		wantScore float64
	}{
		{
			name:    "no answers",
			body:    `{"answers":[]}`,
			wantErr: "OpenAI response has 0 answers, want 1",
		},
		{
			name:    "no answers field",
			body:    `{"model":"gpt-6-luna"}`,
			wantErr: "OpenAI response has 0 answers, want 1",
		},
		{
			name: "two answers",
			body: `{"answers":[{"type":"predicate","name":"condition","probability":0.9},` +
				`{"type":"predicate","name":"condition","probability":0.1}]}`,
			wantErr: "OpenAI response has 2 answers, want 1",
		},
		{
			name:    "an answer to another question",
			body:    `{"answers":[{"type":"predicate","name":"relevance","probability":0.9}]}`,
			wantErr: `OpenAI answer is for "relevance", want "condition"`,
		},
		{
			name:    "an answer without a name",
			body:    `{"answers":[{"type":"predicate","probability":0.9}]}`,
			wantErr: `OpenAI answer is for "", want "condition"`,
		},
		{
			name:    "a name above 512 bytes is cut",
			body:    `{"answers":[{"type":"predicate","name":"` + strings.Repeat("x", 600) + `","probability":0.9}]}`,
			wantErr: `OpenAI answer is for "` + strings.Repeat("x", 512) + `", want "condition"`,
		},
		{
			name:    "a type above 512 bytes is cut",
			body:    `{"answers":[{"type":"` + strings.Repeat("x", 600) + `","name":"condition","probability":0.9}]}`,
			wantErr: `OpenAI answer has type "` + strings.Repeat("x", 512) + `", want "predicate"`,
		},
		{
			name:    "a refusal of another question",
			body:    `{"answers":[{"type":"refusal","name":"relevance"}]}`,
			wantErr: `OpenAI answer is for "relevance", want "condition"`,
		},
		{
			name:    "a choice answer",
			body:    `{"answers":[{"type":"choice","name":"condition","choice":"yes","confidence":0.9}]}`,
			wantErr: `OpenAI answer has type "choice", want "predicate"`,
		},
		{
			name:    "no probability",
			body:    `{"answers":[{"type":"predicate","name":"condition"}]}`,
			wantErr: "OpenAI answer has no probability",
		},
		{
			name:    "a null probability",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":null}]}`,
			wantErr: "OpenAI answer has no probability",
		},
		{
			name:    "probability above 1",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":1.5}]}`,
			wantErr: "OpenAI answer has probability 1.5, outside [0, 1]",
		},
		{
			name:    "probability below 0",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":-0.1}]}`,
			wantErr: "OpenAI answer has probability -0.1, outside [0, 1]",
		},
		{
			name:      "probability 0",
			body:      `{"answers":[{"type":"predicate","name":"condition","probability":0}]}`,
			wantScore: 0,
		},
		{
			name:      "probability 1",
			body:      `{"answers":[{"type":"predicate","name":"condition","probability":1}]}`,
			wantScore: 1,
		},
		{
			// OpenAI refuses a document whose content has no evidence for the condition.
			name:      "a refusal",
			body:      `{"answers":[{"type":"refusal","name":"condition"}]}`,
			wantScore: 0,
		},
		{
			name:    "not JSON",
			body:    `<html>`,
			wantErr: "parse OpenAI response: invalid character '<' looking for beginning of value: <html>",
		},
		{
			name:    "a body above the limit",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":0.5}]}` + strings.Repeat(" ", maxResponseBodyBytes),
			wantErr: "OpenAI response exceeds 1048576 bytes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}

			res, err := newTestClient(defaultFakeKey).Rank(context.Background(), churn, "text",
				[]string{"", noRefund}, classConfig(f.url(), nil))

			assert.Len(t, f.received(), 1)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, res)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []ent.DocumentScore{{Document: "", Score: 0}, {Document: noRefund, Score: tt.wantScore}},
				res.DocumentScores)
		})
	}
}

func TestRankDoesNotFailOnTheUsage(t *testing.T) {
	const answers = `"answers":[{"type":"predicate","name":"condition","probability":0.7}]`
	tests := []struct {
		name string
		body string
	}{
		{name: "a whole number", body: `{` + answers + `,"usage":{"input_tokens":120,"output_tokens":0}}`},
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

func nullLogger() logrus.FieldLogger {
	logger, _ := test.NewNullLogger()
	return logger
}

func debugLogger() (logrus.FieldLogger, *test.Hook) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return logger, hook
}

func newTestClient(apiKey string) *client {
	return New(apiKey, "", testTimeout, MaxDocumentsPerQuery, nullLogger())
}

func numbered(format string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i)
	}
	return out
}

func decisionBody(t *testing.T, model, input, query string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"model": model,
		"input": input,
		"questions": []map[string]string{
			{"type": "predicate", "name": "condition", "instructions": query},
		},
	})
	require.NoError(t, err)
	return string(body)
}

func inputs(requests []fakeRequest) []string {
	out := make([]string, len(requests))
	for i, r := range requests {
		out[i] = r.input()
	}
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
	Name         string `json:"name"`
	Instructions string `json:"instructions"`
}

// wireRequest uses the API's field names, independent of the client's types.
type wireRequest struct {
	Model     string         `json:"model"`
	Input     any            `json:"input"`
	Questions []wireQuestion `json:"questions"`
}

type fakeRequest struct {
	method        string
	path          string
	contentType   string
	authorization string
	organization  []string
	rawBody       string
	body          wireRequest
}

func (r fakeRequest) input() string {
	switch input := r.body.Input.(type) {
	case string:
		return input
	case []any:
		var wire []struct {
			Role    string `json:"role"`
			Content []struct {
				Type     string `json:"type"`
				ImageURL string `json:"image_url"`
			} `json:"content"`
		}
		if err := json.Unmarshal([]byte(r.rawInput()), &wire); err != nil || len(wire) != 1 || len(wire[0].Content) != 1 {
			return ""
		}
		return wire[0].Content[0].ImageURL
	}
	return ""
}

func (r fakeRequest) rawInput() string {
	raw, _ := json.Marshal(r.body.Input)
	return string(raw)
}

type fakeReply struct {
	status int
	body   string
	// stalled sends the status and the headers, and never the body.
	stalled bool
	// hangUp closes the connection after it read the request, without a reply.
	hangUp bool
}

// fakeOpenAI answers with the probability of the input, 0.5 when unknown, or
// with a refusal for an input in refusals.
type fakeOpenAI struct {
	t             *testing.T
	server        *httptest.Server
	probabilities map[string]float64
	refusals      map[string]bool
	// replies overrides the answer to the n-th request, from 0.
	replies map[int]fakeReply
	delay   time.Duration
	// together, when set, holds each handler until that many requests are in
	// their handlers, so every one of them needs a connection of its own.
	together int
	release  chan struct{}
	handlers sync.WaitGroup
	// closed ends the handlers of stalled replies.
	closed chan struct{}

	// opened counts the connections the server accepted.
	opened atomic.Int64

	lock     sync.Mutex
	requests []fakeRequest
	// gathering closes when together requests have arrived; waiting counts
	// the requests that wait for it.
	gathering chan struct{}
	waiting   int
}

func newFake(t *testing.T) *fakeOpenAI {
	f := &fakeOpenAI{t: t, closed: make(chan struct{})}
	f.server = httptest.NewUnstartedServer(f)
	f.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.opened.Add(1)
		}
	}
	f.server.Start()
	t.Cleanup(f.server.Close)
	t.Cleanup(func() { close(f.closed) })
	return f
}

func (f *fakeOpenAI) url() string {
	return f.server.URL
}

func (f *fakeOpenAI) received() []fakeRequest {
	f.lock.Lock()
	defer f.lock.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeOpenAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.handlers.Add(1)
	defer f.handlers.Done()

	rawBody, err := io.ReadAll(r.Body)
	if !assert.NoError(f.t, err) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var body wireRequest
	if !assert.NoError(f.t, json.Unmarshal(rawBody, &body)) || !assert.Len(f.t, body.Questions, 1) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	received := fakeRequest{
		method:        r.Method,
		path:          r.URL.Path,
		contentType:   r.Header.Get("Content-Type"),
		authorization: r.Header.Get("Authorization"),
		organization:  r.Header.Values("OpenAI-Organization"),
		rawBody:       string(rawBody),
		body:          body,
	}

	f.lock.Lock()
	reply, replied := f.replies[len(f.requests)]
	f.requests = append(f.requests, received)
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

	if replied && reply.hangUp {
		conn, _, err := w.(http.Hijacker).Hijack()
		if assert.NoError(f.t, err) {
			conn.Close()
		}
		return
	}
	if replied && reply.stalled {
		w.Header().Set("Content-Length", "1024")
		w.WriteHeader(reply.status)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-f.closed:
		}
		return
	}
	if replied {
		w.WriteHeader(reply.status)
		io.WriteString(w, reply.body)
		return
	}
	answer := map[string]any{"type": "refusal", "name": body.Questions[0].Name}
	if !f.refusals[received.input()] {
		probability, ok := f.probabilities[received.input()]
		if !ok {
			probability = 0.5
		}
		answer = map[string]any{"type": "predicate", "name": body.Questions[0].Name, "probability": probability}
	}
	json.NewEncoder(w).Encode(map[string]any{
		"model":   body.Model,
		"answers": []map[string]any{answer},
		"usage":   map[string]int{"input_tokens": 100, "output_tokens": 0},
	})
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
