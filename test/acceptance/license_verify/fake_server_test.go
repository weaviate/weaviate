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

package license_verify

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/weaviate/weaviate/entities/license"
)

const testServerKeyID = "license-m5-test"

type responseMode int

const (
	respondValid responseMode = iota
	respondWithWrongServerKey
	respondWithEditedStatus
	respondWithWrongNonce
)

type fakeLicenseServer struct {
	server *httptest.Server
	port   int

	customerPublicKey ed25519.PublicKey
	serverPublicKey   ed25519.PublicKey
	serverPrivateKey  ed25519.PrivateKey
	wrongPrivateKey   ed25519.PrivateKey
	expiresAt         time.Time

	mu            sync.Mutex
	mode          responseMode
	calls         int
	requests      []protocol.VerifyRequest
	responses     []protocol.VerifyResponse
	handlerErrors []error
}

func newFakeLicenseServer(t *testing.T, customerPublicKey ed25519.PublicKey, expiresAt time.Time) *fakeLicenseServer {
	t.Helper()

	serverPublicKey, serverPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, wrongPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)

	s := &fakeLicenseServer{
		port:              ln.Addr().(*net.TCPAddr).Port,
		customerPublicKey: customerPublicKey,
		serverPublicKey:   serverPublicKey,
		serverPrivateKey:  serverPrivateKey,
		wrongPrivateKey:   wrongPrivateKey,
		expiresAt:         expiresAt.UTC().Truncate(time.Second),
	}
	s.server = &httptest.Server{
		Listener: ln,
		Config:   &http.Server{Handler: http.HandlerFunc(s.handle)},
	}
	s.server.Start()
	return s
}

func (s *fakeLicenseServer) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.calls++
	mode := s.mode
	s.mu.Unlock()

	if r.Method != http.MethodPost {
		s.reject(w, http.StatusMethodNotAllowed, fmt.Errorf("unexpected method %s", r.Method))
		return
	}
	if r.URL.Path != "/v1/verify" {
		s.reject(w, http.StatusNotFound, fmt.Errorf("unexpected path %s", r.URL.Path))
		return
	}

	var req protocol.VerifyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.reject(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	if err := req.VerifySignature(s.customerPublicKey); err != nil {
		s.reject(w, http.StatusUnauthorized, fmt.Errorf("verify request signature: %w", err))
		return
	}
	if err := req.CheckFreshness(time.Now()); err != nil {
		s.reject(w, http.StatusBadRequest, err)
		return
	}

	now := time.Now().UTC()
	resp := protocol.VerifyResponse{
		LicenseID:      req.LicenseID,
		Status:         protocol.StatusValid,
		ExpiresAt:      s.expiresAt,
		CheckedAt:      now,
		NextCheckAfter: now.Add(time.Hour),
		Nonce:          req.Nonce,
		Entitlements:   []string{protocol.EntitlementEnterprise},
	}

	signingKey := protocol.ServerKey{ID: testServerKeyID, PrivateKey: s.serverPrivateKey}
	switch mode {
	case respondValid, respondWithEditedStatus:
	case respondWithWrongServerKey:
		signingKey = protocol.ServerKey{ID: "license-m5-untrusted", PrivateKey: s.wrongPrivateKey}
	case respondWithWrongNonce:
		resp.Nonce += "-forged"
	}
	if err := signingKey.Sign(&resp); err != nil {
		s.reject(w, http.StatusInternalServerError, fmt.Errorf("sign response: %w", err))
		return
	}
	if mode == respondWithEditedStatus {
		resp.Status = protocol.StatusRevoked
	}

	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.responses = append(s.responses, resp)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-License-Protocol", "1")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.mu.Lock()
		s.handlerErrors = append(s.handlerErrors, fmt.Errorf("encode response: %w", err))
		s.mu.Unlock()
	}
}

func (s *fakeLicenseServer) reject(w http.ResponseWriter, status int, err error) {
	s.mu.Lock()
	s.handlerErrors = append(s.handlerErrors, err)
	s.mu.Unlock()
	http.Error(w, http.StatusText(status), status)
}

func (s *fakeLicenseServer) url() string {
	return fmt.Sprintf("http://host.docker.internal:%d", s.port)
}

func (s *fakeLicenseServer) trustedKeys() string {
	return testServerKeyID + ":" + base64.RawURLEncoding.EncodeToString(s.serverPublicKey)
}

func (s *fakeLicenseServer) setMode(mode responseMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
}

func (s *fakeLicenseServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeLicenseServer) requestCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func (s *fakeLicenseServer) firstRequest() (protocol.VerifyRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return protocol.VerifyRequest{}, false
	}
	return s.requests[0], true
}

func (s *fakeLicenseServer) firstResponse() (protocol.VerifyResponse, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.responses) == 0 {
		return protocol.VerifyResponse{}, false
	}
	return s.responses[0], true
}

func (s *fakeLicenseServer) requireNoHandlerErrors(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Empty(t, s.handlerErrors)
}

func (s *fakeLicenseServer) close() {
	s.mu.Lock()
	server := s.server
	s.server = nil
	s.mu.Unlock()
	if server != nil {
		server.Close()
	}
}

func (s *fakeLicenseServer) reopen(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", s.port))
	require.NoError(t, err)
	server := &httptest.Server{
		Listener: ln,
		Config:   &http.Server{Handler: http.HandlerFunc(s.handle)},
	}
	server.Start()
	s.mu.Lock()
	s.server = server
	s.mu.Unlock()
}
