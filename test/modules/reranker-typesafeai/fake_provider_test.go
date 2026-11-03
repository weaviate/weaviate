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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const fakeAPIKey = "fake"

// wireRequest decodes the request body with the API's field names,
// independent of the module's types.
type wireRequest struct {
	Model     string                  `json:"model"`
	State     any                     `json:"state"`
	Questions map[string]wireQuestion `json:"questions"`
	Assets    map[string]wireAsset    `json:"assets"`
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

type fakeRequest struct {
	path          string
	authorization string
	imagesHeader  string
	body          wireRequest
}

// fakeReply answers a request with a status and a body instead of the
// probabilities.
type fakeReply struct {
	status int
	body   string
}

// fakeProvider stands for POST /v1/systemone of the TypeSafe AI API. It
// listens on every interface of the host, which the Weaviate container
// reaches as host.docker.internal.
type fakeProvider struct {
	server *httptest.Server
	port   int

	mu            sync.Mutex
	probabilities map[string]float64
	replies       map[int]fakeReply
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

// reset forgets the requests received and sets how the next ones are
// answered: probabilities holds the probability of each document, a text or
// the base64 of an image, and replies answers the n-th request, counted from
// 0, instead.
func (f *fakeProvider) reset(probabilities map[string]float64, replies map[int]fakeReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.probabilities = probabilities
	f.replies = replies
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
	reply, replied := f.replies[len(f.requests)]
	probabilities := f.probabilities
	f.requests = append(f.requests, fakeRequest{
		path:          r.URL.Path,
		authorization: r.Header.Get("Authorization"),
		imagesHeader:  r.Header.Get("TypeSafe-Danger-Zone-Images"),
		body:          body,
	})
	f.mu.Unlock()

	if replied {
		w.WriteHeader(reply.status)
		io.WriteString(w, reply.body)
		return
	}
	if decodeErr != nil {
		http.Error(w, fmt.Sprintf("fake: decode request: %v", decodeErr), http.StatusBadRequest)
		return
	}
	answers, err := answer(body, probabilities)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"answers": answers,
		"usage":   map[string]int{"input_tokens": 100},
	})
}

// answer gives every question of a request of several documents the
// probability of its document: the text in the state under the question's
// key, or the image that text names.
func answer(body wireRequest, probabilities map[string]float64) (map[string]any, error) {
	state, _ := body.State.(map[string]any)
	answers := make(map[string]any, len(body.Questions))
	for key := range body.Questions {
		document, _ := state[key].(string)
		if asset, ok := body.Assets[document]; ok {
			document = asset.DataBase64
		}
		probability, ok := probabilities[document]
		if !ok {
			return nil, fmt.Errorf("fake: no probability for the document of question %q", key)
		}
		answers[key] = map[string]any{"type": "noul", "noul": probability}
	}
	return answers, nil
}
