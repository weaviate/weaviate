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

// Package credentialtest helps module client tests check which credentials
// reach which host.
package credentialtest

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// Server is a TLS test server that records every request it receives and
// answers each one with the JSON encoding of a fixed response.
type Server struct {
	*httptest.Server
	mu       sync.Mutex
	requests []*http.Request
}

func NewServer(t *testing.T, response any) *Server {
	t.Helper()
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	s := &Server{}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.Clone(context.Background()))
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	t.Cleanup(s.Close)
	return s
}

// Requests returns the requests received so far.
func (s *Server) Requests() []*http.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*http.Request(nil), s.requests...)
}

// ClientForAnyHost returns a client that sends every request to the server,
// whatever host its URL names. The server sees the host the URL named in
// Request.Host, so a test can follow a URL built for a provider host such as
// corp.openai.azure.com and check where the request was meant to go.
func (s *Server) ClientForAnyHost() *http.Client {
	transport := s.Client().Transport.(*http.Transport).Clone()
	addr := s.Listener.Addr().String()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, network, addr)
	}
	// The httptest certificate is valid for example.com, not for the host the
	// URL names.
	transport.TLSClientConfig.ServerName = "example.com"
	return &http.Client{Transport: transport}
}

// HostPort is the server address without the scheme.
func (s *Server) HostPort() string {
	return strings.TrimPrefix(s.URL, "https://")
}

// Context returns a context that stores each header as a []string under its
// canonical name (X-Contextualai-Api-Key for X-ContextualAI-Api-Key), as the
// REST middleware does. A module that looks a header up under another
// spelling does not find it over REST, and its test fails.
func Context(headers map[string]string) context.Context {
	ctx := context.Background()
	for k, v := range headers {
		ctx = context.WithValue(ctx, http.CanonicalHeaderKey(k), []string{v})
	}
	return ctx
}

// ClassConfig is a moduletools.ClassConfig whose module settings are the map.
type ClassConfig map[string]any

var _ moduletools.ClassConfig = ClassConfig(nil)

func (c ClassConfig) TargetVector() string                            { return "" }
func (c ClassConfig) Tenant() string                                  { return "" }
func (c ClassConfig) Class() map[string]any                           { return c }
func (c ClassConfig) ClassByModuleName(string) map[string]any         { return c }
func (c ClassConfig) Property(string) map[string]any                  { return nil }
func (c ClassConfig) PropertiesDataTypes() map[string]schema.DataType { return nil }
func (c ClassConfig) Config() *config.Config                          { return nil }
