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

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/tokenizer"
)

func TestHandleGenericTokenize(t *testing.T) {
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name          string
		request       tokenizeRequest
		wantStatus    int
		wantIndexed   []string
		wantQuery     []string
		wantErrSubstr string
	}{
		{
			name: "word tokenization without stopwords",
			request: tokenizeRequest{
				Text:         "The quick brown fox",
				Tokenization: "word",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"the", "quick", "brown", "fox"},
			wantQuery:   []string{"the", "quick", "brown", "fox"},
		},
		{
			name: "word tokenization with english stopwords",
			request: tokenizeRequest{
				Text:         "The quick brown fox jumps over the lazy dog",
				Tokenization: "word",
				AnalyzerConfig: &analyzerConfig{
					Stopwords: &models.StopwordConfig{
						Preset: "en",
					},
				},
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"},
			wantQuery:   []string{"quick", "brown", "fox", "jumps", "over", "lazy", "dog"},
		},
		{
			name: "lowercase tokenization",
			request: tokenizeRequest{
				Text:         "Hello World Test",
				Tokenization: "lowercase",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"hello", "world", "test"},
			wantQuery:   []string{"hello", "world", "test"},
		},
		{
			name: "whitespace tokenization",
			request: tokenizeRequest{
				Text:         "Hello World Test",
				Tokenization: "whitespace",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"Hello", "World", "Test"},
			wantQuery:   []string{"Hello", "World", "Test"},
		},
		{
			name: "field tokenization",
			request: tokenizeRequest{
				Text:         "  Hello World  ",
				Tokenization: "field",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"Hello World"},
			wantQuery:   []string{"Hello World"},
		},
		{
			name: "trigram tokenization",
			request: tokenizeRequest{
				Text:         "Hello",
				Tokenization: "trigram",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"hel", "ell", "llo"},
			wantQuery:   []string{"hel", "ell", "llo"},
		},
		{
			name: "missing text",
			request: tokenizeRequest{
				Tokenization: "word",
			},
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "text is required",
		},
		{
			name: "missing tokenization",
			request: tokenizeRequest{
				Text: "hello",
			},
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "tokenization is required",
		},
		{
			name: "unknown tokenization",
			request: tokenizeRequest{
				Text:         "hello",
				Tokenization: "unknown",
			},
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "unknown tokenization",
		},
		{
			name: "invalid stopwords preset",
			request: tokenizeRequest{
				Text:         "hello",
				Tokenization: "word",
				AnalyzerConfig: &analyzerConfig{
					Stopwords: &models.StopwordConfig{
						Preset: "invalid",
					},
				},
			},
			wantStatus:    http.StatusBadRequest,
			wantErrSubstr: "invalid stopwords config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.request)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/v1/tokenize", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handleGenericTokenize(w, req, logger)

			assert.Equal(t, tt.wantStatus, w.Code)

			if tt.wantStatus == http.StatusOK {
				var resp tokenizeResponse
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				require.NoError(t, err)
				assert.Equal(t, tt.request.Tokenization, resp.Tokenization)
				assert.Equal(t, tt.wantIndexed, resp.Indexed)
				assert.Equal(t, tt.wantQuery, resp.Query)
			} else if tt.wantErrSubstr != "" {
				assert.Contains(t, w.Body.String(), tt.wantErrSubstr)
			}
		})
	}
}

func TestFilterStopwords(t *testing.T) {
	tests := []struct {
		name     string
		tokens   []string
		config   models.StopwordConfig
		expected []string
	}{
		{
			name:     "english preset",
			tokens:   []string{"the", "quick", "brown", "fox"},
			config:   models.StopwordConfig{Preset: "en"},
			expected: []string{"quick", "brown", "fox"},
		},
		{
			name:     "no preset",
			tokens:   []string{"the", "quick", "brown", "fox"},
			config:   models.StopwordConfig{Preset: "none"},
			expected: []string{"the", "quick", "brown", "fox"},
		},
		{
			name:     "custom additions",
			tokens:   []string{"hello", "world", "test"},
			config:   models.StopwordConfig{Preset: "none", Additions: []string{"test"}},
			expected: []string{"hello", "world"},
		},
		{
			name:     "english with removals",
			tokens:   []string{"the", "quick"},
			config:   models.StopwordConfig{Preset: "en", Removals: []string{"the"}},
			expected: []string{"the", "quick"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detector, err := stopwordsFromConfig(tt.config)
			require.NoError(t, err)
			result := filterStopwords(tt.tokens, detector)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func stopwordsFromConfig(config models.StopwordConfig) (*stopwords.Detector, error) {
	return stopwords.NewDetectorFromConfig(config)
}

func TestHandleGenericTokenizeGSE(t *testing.T) {
	t.Setenv("USE_GSE", "true")
	t.Setenv("ENABLE_TOKENIZER_GSE_CH", "true")
	tokenizer.InitOptionalTokenizers()

	logger, _ := test.NewNullLogger()

	tests := []struct {
		name        string
		request     tokenizeRequest
		wantStatus  int
		wantIndexed []string
	}{
		{
			name: "gse Japanese tokenization",
			request: tokenizeRequest{
				Text:         "素早い茶色の狐が怠けた犬を飛び越えた",
				Tokenization: "gse",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"素早", "素早い", "早い", "茶色", "の", "狐", "が", "怠け", "けた", "犬", "を", "飛び", "飛び越え", "越え", "た"},
		},
		{
			name: "gse_ch Chinese tokenization",
			request: tokenizeRequest{
				Text:         "施氏食狮史",
				Tokenization: "gse_ch",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"施", "氏", "食", "狮", "史"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.request)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/v1/tokenize", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handleGenericTokenize(w, req, logger)

			assert.Equal(t, tt.wantStatus, w.Code)

			if tt.wantStatus == http.StatusOK {
				var resp tokenizeResponse
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				require.NoError(t, err)
				assert.Equal(t, tt.request.Tokenization, resp.Tokenization)
				assert.Equal(t, tt.wantIndexed, resp.Indexed)
			}
		})
	}
}

func TestHandleGenericTokenizeKagome(t *testing.T) {
	t.Setenv("ENABLE_TOKENIZER_KAGOME_KR", "true")
	t.Setenv("ENABLE_TOKENIZER_KAGOME_JA", "true")
	tokenizer.InitOptionalTokenizers()

	logger, _ := test.NewNullLogger()

	tests := []struct {
		name        string
		request     tokenizeRequest
		wantStatus  int
		wantIndexed []string
	}{
		{
			name: "kagome_kr Korean tokenization",
			request: tokenizeRequest{
				Text:         "아버지가 방에 들어가신다",
				Tokenization: "kagome_kr",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"아버지", "가", "방", "에", "들어가", "신다"},
		},
		{
			name: "kagome_kr Korean without spaces",
			request: tokenizeRequest{
				Text:         "한국어를처리하는예시입니다",
				Tokenization: "kagome_kr",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"한국어", "를", "처리", "하", "는", "예시", "입니다"},
		},
		{
			name: "kagome_ja Japanese tokenization",
			request: tokenizeRequest{
				Text:         "素早い茶色の狐が怠けた犬を飛び越えた",
				Tokenization: "kagome_ja",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"素早い", "茶色", "の", "狐", "が", "怠け", "た", "犬", "を", "飛び越え", "た"},
		},
		{
			name: "kagome_ja English text passthrough",
			request: tokenizeRequest{
				Text:         "The quick brown fox jumps over the lazy dog",
				Tokenization: "kagome_ja",
			},
			wantStatus:  http.StatusOK,
			wantIndexed: []string{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := json.Marshal(tt.request)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/v1/tokenize", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handleGenericTokenize(w, req, logger)

			assert.Equal(t, tt.wantStatus, w.Code)

			if tt.wantStatus == http.StatusOK {
				var resp tokenizeResponse
				err := json.Unmarshal(w.Body.Bytes(), &resp)
				require.NoError(t, err)
				assert.Equal(t, tt.request.Tokenization, resp.Tokenization)
				assert.Equal(t, tt.wantIndexed, resp.Indexed)
			}
		})
	}
}

func TestTokenizeMiddlewareRouting(t *testing.T) {
	logger, _ := test.NewNullLogger()

	fallbackCalled := false
	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := addTokenizeHandlers(nil, logger, fallback)

	tests := []struct {
		name         string
		method       string
		path         string
		wantFallback bool
	}{
		{
			name:         "POST /v1/tokenize is handled",
			method:       http.MethodPost,
			path:         "/v1/tokenize",
			wantFallback: false,
		},
		{
			name:         "GET /v1/tokenize falls through",
			method:       http.MethodGet,
			path:         "/v1/tokenize",
			wantFallback: true,
		},
		{
			name:         "unrelated path falls through",
			method:       http.MethodPost,
			path:         "/v1/objects",
			wantFallback: true,
		},
		// Note: property tokenize path matching is tested but the handler
		// needs a real schema manager, so we only verify routing pattern here
		// by checking non-matching paths fall through.
		{
			name:         "non-tokenize schema path falls through",
			method:       http.MethodGet,
			path:         "/v1/schema/Articles",
			wantFallback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fallbackCalled = false

			body := bytes.NewReader([]byte(`{"text":"hello","tokenization":"word"}`))
			req := httptest.NewRequest(tt.method, tt.path, body)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			assert.Equal(t, tt.wantFallback, fallbackCalled, "fallback handler called")
		})
	}
}
