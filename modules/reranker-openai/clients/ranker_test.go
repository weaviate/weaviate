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
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-openapi/strfmt"
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
	firstID        = "00000000-0000-0000-0000-000000000000"
	// The empty document is not sent, so an error naming secondID names the
	// request's document, not its position.
	secondID = "00000000-0000-0000-0000-000000000001"
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

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(documents...), classConfig(f.url(), tt.settings))

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

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(documents...), classConfig(f.url(), nil))

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

// The shared default transport keeps two idle connections per host, and the
// module sends one request per document at once, so a query opened most of
// its connections anew.
func TestRankReusesConnectionsAcrossQueries(t *testing.T) {
	f := newFake(t)
	f.delay = 50 * time.Millisecond
	documents := make([]string, 20)
	for i := range documents {
		documents[i] = fmt.Sprintf("ticket %d", i)
	}
	c := newTestClient(defaultFakeKey)
	rank := func() {
		_, err := c.RankProperty(context.Background(), churn, "text", documentsOf(documents...), classConfig(f.url(), nil))
		require.NoError(t, err)
	}

	rank()
	opened := f.opened.Load()
	require.Greater(t, opened, int64(2), "the first query must send requests at once")
	rank()

	assert.Equal(t, opened, f.opened.Load(), "the second query opened new connections")
}

func TestRankSkipsEmptyAndDuplicateDocuments(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"a": 0.8, "b": 0.2}
	logger, hook := debugLogger()
	c := New(defaultFakeKey, "", testTimeout, DefaultMaxConcurrentRequests, logger)

	res, err := c.RankProperty(context.Background(), churn, "text", documentsOf("", "a", "a", "b"), classConfig(f.url(), nil))

	require.NoError(t, err)
	assert.Equal(t, []ent.DocumentScore{
		{Document: "", Score: 0},
		{Document: "a", Score: 0.8},
		{Document: "a", Score: 0.8},
		{Document: "b", Score: 0.2},
	}, res.DocumentScores)
	assert.ElementsMatch(t, []string{"a", "b"}, inputs(f.received()))
	assert.Equal(t, logrus.Fields{"documents": 4, "judged": 2, "input_tokens": int64(200)}, rankLogFields(t, hook))
}

func TestRankScoresAWhitespaceOnlyDocumentZeroWithoutSendingIt(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"a": 0.8, "b": 0.2}

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(" ", "a", "\t\n ", "b"), classConfig(f.url(), nil))

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

	require.EqualError(t, err, "rerank.query is required by reranker-openai: the query is the condition the model judges")
	assert.Empty(t, f.received())
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

			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), strings.Repeat("q", tt.queryBytes), "text",
				documentsOf(noRefund), classConfig(f.url(), nil))

			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Len(t, f.received(), tt.wantRequests)
		})
	}
}

func TestRankChecksTheStoredSettings(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{name: "a baseURL that is a number", settings: map[string]any{"baseURL": 42}, wantErr: "baseURL must be a string, got int"},
		{name: "a baseURL that is a list", settings: map[string]any{"baseURL": []any{"a"}}, wantErr: "baseURL must be a string, got []interface {}"},
		{name: "an empty baseURL", settings: map[string]any{"baseURL": ""}, wantErr: "baseURL must be a non-empty string"},
		{name: "a model that is a number", settings: map[string]any{"model": 7}, wantErr: "model must be a string, got int"},
		{name: "a model that is a bool", settings: map[string]any{"model": true}, wantErr: "model must be a string, got bool"},
		{name: "an empty model", settings: map[string]any{"model": ""}, wantErr: "model must be a non-empty string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			c := newTestClient(defaultFakeKey)
			sent := &countingTransport{}
			c.httpClient.Transport = sent

			_, err := c.RankProperty(context.Background(), churn, "text",
				documentsOf(noRefund), classConfig(f.url(), tt.settings))

			require.EqualError(t, err, tt.wantErr)
			assert.Zero(t, sent.count.Load(), "no request left the client")
		})
	}
}

// countingTransport fails every request, so a stored baseURL that fell back to
// the default host is seen without reaching it.
type countingTransport struct{ count atomic.Int64 }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.count.Add(1)
	return nil, errors.New("unexpected request")
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
			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, tt.property,
				documentsOf(noRefund), tt.cfg)

			require.EqualError(t, err, tt.wantErr)
		})
	}
	assert.Empty(t, f.received())
}

func TestRankSendsABlobPropertyAsAnImage(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{"data:image/png;base64," + pngBase64: 0.8, "data:image/jpeg;base64," + jpegBase64: 0.2}
	documents := []string{pngBase64, jpegBase64}

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), "The product has visible damage", "photo",
		documentsOf(documents...), classConfig(f.url(), nil))

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
			wantErr:   "object 00000000-0000-0000-0000-000000000001: image: not a PNG, JPEG, GIF or WebP image",
		},
		{
			name:      "a BMP",
			documents: []string{pngBase64, bmpBase64},
			wantErr:   "object 00000000-0000-0000-0000-000000000001: image: not a PNG, JPEG, GIF or WebP image",
		},
		{
			name:      "not base64",
			documents: []string{pngBase64, "%%%%"},
			wantErr:   "object 00000000-0000-0000-0000-000000000001: image: not base64",
		},
		{
			name:      "20 MiB + 1 byte",
			documents: []string{pngBase64, pngOfSize(maxImageBytes + 1)},
			wantErr:   "object 00000000-0000-0000-0000-000000000001: image: 20971521 bytes exceed the limit of 20 MiB per image",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "photo",
				documentsOf(tt.documents...), classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
		})
	}
}

func TestRankSendsBase64TextAsText(t *testing.T) {
	f := newFake(t)

	_, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
		documentsOf(pngBase64), classConfig(f.url(), nil))

	require.NoError(t, err)
	requests := f.received()
	require.Len(t, requests, 1)
	assert.Equal(t, pngBase64, requests[0].body.Input)
}

// Classes created before v1.19 still store the deprecated string type.
func TestRankReadsTheDeprecatedStringTypeAsText(t *testing.T) {
	f := newFake(t)
	f.probabilities = map[string]float64{noRefund: 0.9}

	res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "legacy",
		documentsOf(noRefund), classConfig(f.url(), nil))

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
			name:    "400 names the object",
			reply:   fakeReply{status: http.StatusBadRequest, body: `{"error":{"message":"Invalid input","type":"invalid_request_error"}}`},
			wantErr: "object " + secondID + ": connection to OpenAI API failed with status 400: Invalid input",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: tt.reply}

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf("", noRefund), classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Nil(t, res)
			assert.Len(t, f.received(), 1)
		})
	}
}

func TestRankStopsAfterTheFirstProviderError(t *testing.T) {
	f := newFake(t)
	f.replies = map[int]fakeReply{0: {status: http.StatusTooManyRequests, body: `{"error":{"message":"slow down"}}`}}

	res, err := New(defaultFakeKey, "", testTimeout, 1, nullLogger()).RankProperty(context.Background(), churn, "text",
		documentsOf("first", "second", "third"), classConfig(f.url(), nil))

	require.EqualError(t, err, "connection to OpenAI API failed with status 429: slow down")
	assert.Nil(t, res)
	assert.Len(t, f.received(), 1)
}

func TestRankRejectsAClassBaseURLBeforeSending(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr string
	}{
		{
			name:    "no scheme",
			baseURL: "api.openai.com",
			wantErr: `baseURL must be an absolute http or https URL, got "api.openai.com"`,
		},
		{
			// <port> stands for the fake's port.
			name:    "the fake's port without a host",
			baseURL: "http://:<port>",
			wantErr: `baseURL must be an absolute http or https URL, got "http://:<port>"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			fakeURL, err := url.Parse(f.url())
			require.NoError(t, err)
			baseURL := strings.ReplaceAll(tt.baseURL, "<port>", fakeURL.Port())
			wantErr := strings.ReplaceAll(tt.wantErr, "<port>", fakeURL.Port())

			_, err = newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(noRefund), classConfig(baseURL, nil))

			require.EqualError(t, err, wantErr)
			assert.Empty(t, f.received())
		})
	}
}

func TestRankReportsARejectedKeyWithoutTheBody(t *testing.T) {
	const secret = "sk-secret"
	tests := []struct {
		name    string
		status  int
		wantErr string
	}{
		{name: "401", status: http.StatusUnauthorized, wantErr: "OpenAI rejected the API key (status 401)"},
		{name: "403", status: http.StatusForbidden, wantErr: "OpenAI rejected the API key (status 403)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {
				status: tt.status,
				body:   `{"error":{"message":"Incorrect API key provided: sk-secret","code":"invalid_api_key"}}`,
			}}
			logger, hook := debugLogger()
			c := New(secret, "", testTimeout, DefaultMaxConcurrentRequests, logger)

			_, err := c.RankProperty(context.Background(), churn, "text", documentsOf(noRefund), classConfig(f.url(), nil))

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

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf(noRefund), classConfig(f.url(), nil))

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
			c := New(tt.serverKey, tt.serverOrganization, testTimeout, DefaultMaxConcurrentRequests, nullLogger())

			_, err := c.RankProperty(ctx, churn, "text", documentsOf(noRefund), classConfig(classServer.url(), nil))

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
			wantErr: `X-Openai-Baseurl header must be an absolute http or https URL, got "api.openai.com"`,
		},
		{
			name:    "a port without a host",
			baseURL: "https://:443",
			wantErr: `X-Openai-Baseurl header must be an absolute http or https URL, got "https://:443"`,
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
			f := newFake(t)
			ctx := context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"headerKey"})
			ctx = context.WithValue(ctx, "X-Openai-Baseurl", []string{strings.ReplaceAll(tt.baseURL, "<fake>", f.url())})

			_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text",
				documentsOf(noRefund), classConfig(f.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Empty(t, f.received())
		})
	}
}

// The OpenAI SDKs write the base URL with a /v1 suffix.
func TestRankNormalizesTheBaseURL(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		fromHeader bool
		wantPath   string
	}{
		{name: "class base URL without a path", path: "", wantPath: "/v1/decisions"},
		{name: "class base URL ending in /v1", path: "/v1", wantPath: "/v1/decisions"},
		{name: "class base URL ending in /v1/", path: "/v1/", wantPath: "/v1/decisions"},
		{name: "class base URL with a gateway path", path: "/gateway", wantPath: "/gateway/v1/decisions"},
		{name: "class base URL with a gateway path ending in /v1", path: "/gateway/v1", wantPath: "/gateway/v1/decisions"},
		{name: "class base URL ending in /v1/v1", path: "/v1/v1", wantPath: "/v1/v1/decisions"},
		{name: "header base URL ending in /v1/v1", path: "/v1/v1", fromHeader: true, wantPath: "/v1/v1/decisions"},
		{name: "header base URL ending in /v1/", path: "/v1/", fromHeader: true, wantPath: "/v1/decisions"},
		{name: "header base URL with a gateway path ending in /v1", path: "/gateway/v1", fromHeader: true, wantPath: "/gateway/v1/decisions"},
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

			_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text",
				documentsOf(noRefund), classConfig(classBaseURL, nil))

			require.NoError(t, err)
			assert.Empty(t, unused.received())
			requests := used.received()
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantPath, requests[0].path)
		})
	}
}

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
			wantErr: "object " + firstID + ": connection to OpenAI API failed with status 400: internal banner",
		},
		{
			name:       "a 400 from the header base URL",
			reply:      fakeReply{status: http.StatusBadRequest, body: banner},
			fromHeader: true,
			wantErr:    "object " + firstID + ": connection to OpenAI API failed with status 400",
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
			wantErr:    "OpenAI API returned an error",
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
			wantErr:    "parse OpenAI response: invalid character 'o' in literal null (expecting 'u')",
		},
		{
			// A reply in the API's own shape has no useful body text, so the
			// answer name is quoted for every host.
			name:       "a wrong answer name from the header base URL",
			reply:      fakeReply{status: http.StatusOK, body: `{"answers":[{"type":"predicate","name":"internal banner","probability":0.5}]}`},
			fromHeader: true,
			wantErr:    "object " + firstID + `: OpenAI answer is for "internal banner", want "condition"`,
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

			_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text",
				documentsOf(noRefund), classConfig(classServer.url(), nil))

			require.EqualError(t, err, tt.wantErr)
			assert.Len(t, used.received(), 1)
			assert.Empty(t, unused.received())
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

			_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text", documentsOf(noRefund),
				classConfig(classServer.url(), nil))

			require.Error(t, err)
			assert.Contains(t, err.Error(), "parse OpenAI response: ")
			assert.LessOrEqual(t, len(err.Error()), 1200)
		})
	}
}

func TestRankReturnsADeadlineErrorAndFreesTheSlots(t *testing.T) {
	f, release := newBlockingFake(t)
	defer release()
	c := New(defaultFakeKey, "", 50*time.Millisecond, 2, nullLogger())

	result := rankInBackground(t, context.Background(), c, f, []string{noRefund, chargedTwice, changeEmail})

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("RankProperty did not fail with the client timeout within 5 seconds")
	}
	assert.Zero(t, len(c.inFlight))
	assert.Zero(t, len(c.inFlightHeaderHost))
}

func TestRankReadsHeadersFromGRPCMetadata(t *testing.T) {
	classHost, headerHost := newFake(t), newFake(t)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"x-openai-api-key", "grpcKey",
		"x-openai-baseurl", headerHost.url(),
		"x-openai-organization", "grpcOrg",
	))

	_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text", documentsOf(noRefund),
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

			_, err := newTestClient(defaultFakeKey).RankProperty(ctx, churn, "text", documentsOf(noRefund),
				classConfig(classServer.url(), nil))

			require.Error(t, err)
			message := err.Error()
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

func TestRankCapsRequestsInFlight(t *testing.T) {
	f := newFake(t)
	f.delay = 20 * time.Millisecond
	c := New(defaultFakeKey, "", testTimeout, 3, nullLogger())
	queries := [][]string{numbered("first %d", 30), numbered("second %d", 30)}

	var wg sync.WaitGroup
	results := make([]*ent.RankResult, len(queries))
	errs := make([]error, len(queries))
	for i, documents := range queries {
		wg.Go(func() {
			results[i], errs[i] = c.RankProperty(context.Background(), churn, "text", documentsOf(documents...),
				classConfig(f.url(), nil))
		})
	}
	wg.Wait()

	for i, documents := range queries {
		require.NoError(t, errs[i])
		assert.Equal(t, expectedScores(documents, nil), results[i].DocumentScores)
	}
	assert.Len(t, f.received(), 60)
	assert.Equal(t, 3, f.maxInFlight())
}

func TestRankStopsOnContextCancel(t *testing.T) {
	const inFlight = 4
	documents := numbered("ticket %d", 40)

	t.Run("with requests in flight", func(t *testing.T) {
		f, release := newBlockingFake(t)
		defer release()
		c := New(defaultFakeKey, "", testTimeout, inFlight, nullLogger())
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

	t.Run("when cancelled before it asks for a slot another query holds", func(t *testing.T) {
		f, release := newBlockingFake(t)
		defer release()
		c := New(defaultFakeKey, "", testTimeout, inFlight, nullLogger())
		other := rankInBackground(t, context.Background(), c, f, documents)
		require.Eventually(t, func() bool { return len(f.received()) == inFlight }, 5*time.Second, time.Millisecond)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		result := rankInBackground(t, ctx, c, f, []string{noRefund})
		cancel()
		requireCanceledWithinASecond(t, result)

		release()
		require.NoError(t, <-other)
		assert.Len(t, f.received(), len(documents), "the cancelled query sent nothing")
	})
}

func TestRankWaitsForASlotUntilTheContextEnds(t *testing.T) {
	tests := []struct {
		name           string
		hostFromHeader bool
	}{
		{name: "the pool of the class and default host", hostFromHeader: false},
		{name: "the pool of a host named by the request header", hostFromHeader: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// synctest.Wait returns only once send is parked on the slot, so
			// the cancel has to wake it.
			synctest.Test(t, func(t *testing.T) {
				c := New(defaultFakeKey, "", testTimeout, 2, nullLogger())
				pool := c.slotsFor(tt.hostFromHeader)
				for range cap(pool) {
					pool <- struct{}{}
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := make(chan error, 1)
				var calls sync.WaitGroup
				calls.Go(func() {
					_, err := c.send(ctx, judgeRequest{
						endpointURL: "http://unused.invalid/v1/decisions", apiKey: defaultFakeKey,
						hostFromHeader: tt.hostFromHeader, query: churn,
					}, noRefund)
					result <- err
				})
				synctest.Wait()
				select {
				case err := <-result:
					t.Fatalf("send returned before the cancel: %v", err)
				default:
				}

				cancel()
				synctest.Wait()
				select {
				case err := <-result:
					require.ErrorIs(t, err, context.Canceled)
				default:
					t.Fatal("send still waits for a slot after the cancel")
				}
				calls.Wait()
				assert.Len(t, pool, cap(pool), "the cancelled call took no slot")
				for range cap(pool) {
					<-pool
				}
			})
		})
	}
}

func TestRankHeaderHostRequestsDoNotHoldTheClassHostSlots(t *testing.T) {
	const slots = 3
	headerHost, release := newBlockingFake(t)
	defer release()
	classHost := newFake(t)
	c := New(defaultFakeKey, "", testTimeout, slots, nullLogger())
	headerCtx := context.WithValue(context.Background(), "X-Openai-Api-Key", []string{"headerKey"})
	headerCtx = context.WithValue(headerCtx, "X-Openai-Baseurl", []string{headerHost.url()})

	blocked := rankInBackground(t, headerCtx, c, headerHost, numbered("ticket %d", 2*slots))
	require.Eventually(t, func() bool { return len(headerHost.received()) == slots }, 5*time.Second, time.Millisecond)
	require.Len(t, c.inFlightHeaderHost, slots)

	classResult := rankInBackground(t, context.Background(), c, classHost, []string{noRefund})
	select {
	case err := <-classResult:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the query to the class host did not complete within 5 seconds while the header-host pool was full")
	}
	assert.Len(t, classHost.received(), 1)
	assert.Len(t, headerHost.received(), slots, "the query to the class host did not reach the header host")

	release()
	require.NoError(t, <-blocked)
	assert.Len(t, headerHost.received(), 2*slots)
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
		{name: "the default", maxConcurrentRequests: DefaultMaxConcurrentRequests, wantCap: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)

			c := New(defaultFakeKey, "", testTimeout, tt.maxConcurrentRequests, nullLogger())

			require.Equal(t, tt.wantCap, cap(c.inFlight))
			require.Equal(t, tt.wantCap, cap(c.inFlightHeaderHost))
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
		body      string
		wantErr   string
		wantScore float64
	}{
		{
			name:    "no answers",
			body:    `{"answers":[]}`,
			wantErr: "object " + secondID + ": OpenAI response has 0 answers, want 1",
		},
		{
			name:    "no answers field",
			body:    `{"model":"gpt-6-luna"}`,
			wantErr: "object " + secondID + ": OpenAI response has 0 answers, want 1",
		},
		{
			name: "two answers",
			body: `{"answers":[{"type":"predicate","name":"condition","probability":0.9},` +
				`{"type":"predicate","name":"condition","probability":0.1}]}`,
			wantErr: "object " + secondID + ": OpenAI response has 2 answers, want 1",
		},
		{
			name:    "an answer to another question",
			body:    `{"answers":[{"type":"predicate","name":"relevance","probability":0.9}]}`,
			wantErr: "object " + secondID + `: OpenAI answer is for "relevance", want "condition"`,
		},
		{
			name:    "an answer without a name",
			body:    `{"answers":[{"type":"predicate","probability":0.9}]}`,
			wantErr: "object " + secondID + `: OpenAI answer is for "", want "condition"`,
		},
		{
			name:    "a name above 512 bytes is cut",
			body:    `{"answers":[{"type":"predicate","name":"` + strings.Repeat("x", 600) + `","probability":0.9}]}`,
			wantErr: "object " + secondID + `: OpenAI answer is for "` + strings.Repeat("x", 512) + `", want "condition"`,
		},
		{
			name:    "a type above 512 bytes is cut",
			body:    `{"answers":[{"type":"` + strings.Repeat("x", 600) + `","name":"condition","probability":0.9}]}`,
			wantErr: "object " + secondID + `: OpenAI answer has type "` + strings.Repeat("x", 512) + `", want "predicate"`,
		},
		{
			name:    "a refusal of another question",
			body:    `{"answers":[{"type":"refusal","name":"relevance"}]}`,
			wantErr: "object " + secondID + `: OpenAI answer is for "relevance", want "condition"`,
		},
		{
			name:    "a choice answer",
			body:    `{"answers":[{"type":"choice","name":"condition","choice":"yes","confidence":0.9}]}`,
			wantErr: "object " + secondID + `: OpenAI answer has type "choice", want "predicate"`,
		},
		{
			name:    "no probability",
			body:    `{"answers":[{"type":"predicate","name":"condition"}]}`,
			wantErr: "object " + secondID + ": OpenAI answer has no probability",
		},
		{
			name:    "a null probability",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":null}]}`,
			wantErr: "object " + secondID + ": OpenAI answer has no probability",
		},
		{
			name:    "probability above 1",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":1.5}]}`,
			wantErr: "object " + secondID + ": OpenAI answer has probability 1.5, outside [0, 1]",
		},
		{
			name:    "probability below 0",
			body:    `{"answers":[{"type":"predicate","name":"condition","probability":-0.1}]}`,
			wantErr: "object " + secondID + ": OpenAI answer has probability -0.1, outside [0, 1]",
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

			res, err := newTestClient(defaultFakeKey).RankProperty(context.Background(), churn, "text",
				documentsOf("", noRefund), classConfig(f.url(), nil))

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
		name            string
		body            string
		wantInputTokens int64
	}{
		{name: "a whole number", body: `{` + answers + `,"usage":{"input_tokens":120,"output_tokens":0}}`, wantInputTokens: 120},
		{name: "a fraction", body: `{` + answers + `,"usage":{"input_tokens":1.5}}`, wantInputTokens: 1},
		{name: "a string", body: `{` + answers + `,"usage":{"input_tokens":"many"}}`},
		{name: "a usage that is not an object", body: `{` + answers + `,"usage":"n/a"}`},
		{name: "no usage", body: `{` + answers + `}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(t)
			f.replies = map[int]fakeReply{0: {status: http.StatusOK, body: tt.body}}
			logger, hook := debugLogger()
			c := New(defaultFakeKey, "", testTimeout, DefaultMaxConcurrentRequests, logger)

			res, err := c.RankProperty(context.Background(), churn, "text", documentsOf(noRefund), classConfig(f.url(), nil))

			require.NoError(t, err)
			assert.Equal(t, []ent.DocumentScore{{Document: noRefund, Score: 0.7}}, res.DocumentScores)
			assert.Equal(t, tt.wantInputTokens, rankLogFields(t, hook)["input_tokens"])
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
	return New(apiKey, "", testTimeout, DefaultMaxConcurrentRequests, nullLogger())
}

// IDs are 00000000-0000-0000-0000-<i in 12 digits>.
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

// A document missing from probabilities gets the fake's default, 0.5.
func expectedScores(documents []string, probabilities map[string]float64) []ent.DocumentScore {
	out := make([]ent.DocumentScore, len(documents))
	for i, document := range documents {
		probability, ok := probabilities[document]
		if !ok {
			probability = 0.5
		}
		out[i] = ent.DocumentScore{Document: document, Score: probability}
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
}

// fakeOpenAI answers with the probability of the input, 0.5 when unknown, or
// with a refusal for an input in refusals.
type fakeOpenAI struct {
	t             *testing.T
	server        *httptest.Server
	probabilities map[string]float64
	refusals      map[string]bool
	// replies overrides the answer to the n-th request, from 0.
	replies  map[int]fakeReply
	delay    time.Duration
	release  chan struct{}
	handlers sync.WaitGroup

	// opened counts the connections the server accepted.
	opened atomic.Int64

	lock     sync.Mutex
	requests []fakeRequest
	inFlight int
	maxSeen  int
}

func newFake(t *testing.T) *fakeOpenAI {
	f := &fakeOpenAI{t: t}
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

func (f *fakeOpenAI) url() string {
	return f.server.URL
}

func (f *fakeOpenAI) received() []fakeRequest {
	f.lock.Lock()
	defer f.lock.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeOpenAI) maxInFlight() int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.maxSeen
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
