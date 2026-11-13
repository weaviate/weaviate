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

package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// wireRequest uses the API's field names, independent of the module's types.
type wireRequest struct {
	Model     string         `json:"model"`
	Input     any            `json:"input"`
	Questions []wireQuestion `json:"questions"`
}

type wireQuestion struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Instructions string `json:"instructions"`
}

type fakeRequest struct {
	path          string
	authorization string
	organization  string
	body          wireRequest
}

type fakeReply struct {
	status int
	body   string
}

// fakeProvider listens on every interface so the Weaviate container reaches it
// as host.docker.internal.
type fakeProvider struct {
	server *httptest.Server
	port   int

	mu            sync.Mutex
	probabilities map[string]float64
	refusals      map[string]bool
	failures      map[string]fakeReply
	requests      []fakeRequest
}

func newFakeProvider(t *testing.T) *fakeProvider {
	t.Helper()
	f := &fakeProvider{}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	f.port = listener.Addr().(*net.TCPAddr).Port
	f.server = &httptest.Server{Listener: listener, Config: &http.Server{Handler: f}}
	f.server.Start()
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeProvider) baseURL() string {
	return fmt.Sprintf("http://host.docker.internal:%d", f.port)
}

// Documents are keyed by text or by the base64 of an image.
func (f *fakeProvider) reset(probabilities map[string]float64, refusals map[string]bool, failures map[string]fakeReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probabilities = probabilities
	f.refusals = refusals
	f.failures = failures
	f.requests = nil
}

func (f *fakeProvider) received() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

func (f *fakeProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body wireRequest
	decodeErr := json.NewDecoder(r.Body).Decode(&body)

	f.mu.Lock()
	failures := f.failures
	probabilities, refusals := f.probabilities, f.refusals
	f.requests = append(f.requests, fakeRequest{
		path:          r.URL.Path,
		authorization: r.Header.Get("Authorization"),
		organization:  r.Header.Get("OpenAI-Organization"),
		body:          body,
	})
	f.mu.Unlock()

	if decodeErr != nil {
		http.Error(w, fmt.Sprintf("fake: decode request: %v", decodeErr), http.StatusBadRequest)
		return
	}
	document := documentOf(body.Input)
	if failure, ok := failures[document]; ok {
		w.WriteHeader(failure.status)
		io.WriteString(w, failure.body)
		return
	}
	answer := map[string]any{"name": "condition"}
	if refusals[document] {
		answer["type"] = "refusal"
	} else {
		probability, ok := probabilities[document]
		if !ok {
			http.Error(w, "fake: no probability for the document", http.StatusBadRequest)
			return
		}
		answer["type"] = "predicate"
		answer["probability"] = probability
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"answers": []any{answer},
		"usage":   map[string]int{"input_tokens": 100},
	})
}

func documentOf(input any) string {
	if text, ok := input.(string); ok {
		return text
	}
	messages, _ := input.([]any)
	for _, message := range messages {
		content, _ := message.(map[string]any)["content"].([]any)
		for _, part := range content {
			url, _ := part.(map[string]any)["image_url"].(string)
			if _, encoded, ok := strings.Cut(url, ";base64,"); ok {
				return encoded
			}
		}
	}
	return ""
}
