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

package main

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang-jwt/jwt/v5"
	"github.com/weaviate/mockoidc"
)

const (
	authCode     = "auth"
	clientSecret = "Secret"
	clientID     = "mock-oidc-test"
)

// namespacedUser is a custom mockoidc.User that emits the namespace and
// global-principal claims alongside the MockUser baseline. Either extra
// claim is omitted when its corresponding zero value is set (empty
// Namespace, nil GlobalPrincipal).
type namespacedUser struct {
	mockoidc.MockUser
	Namespace       string
	GlobalPrincipal *bool
}

// namespacedClaims embeds *mockoidc.IDTokenClaims (concrete pointer, not
// the jwt.Claims interface) so JSON marshalling flattens the standard
// claims (sub, iss, exp, …) alongside the WS-specific extras. Embedding
// the interface instead would nest everything under a single "Claims"
// field and break the wire format.
//
// Both extras use omitempty/pointer-omitempty so an unset value never
// appears on the wire — the OIDC classifier treats missing-key and
// empty-string namespace as equivalent, but having an empty key in the
// JSON would still be surprising and make test debugging harder.
type namespacedClaims struct {
	*mockoidc.IDTokenClaims
	Groups          []string `json:"groups,omitempty"`
	Namespace       string   `json:"weaviate_namespace,omitempty"`
	GlobalPrincipal *bool    `json:"weaviate_global_principal,omitempty"`
}

func (u *namespacedUser) Claims(scope []string, c *mockoidc.IDTokenClaims) (jwt.Claims, error) {
	return &namespacedClaims{
		IDTokenClaims:   c,
		Groups:          u.Groups,
		Namespace:       u.Namespace,
		GlobalPrincipal: u.GlobalPrincipal,
	}, nil
}

// boolPtr is a small helper to construct *bool values inline.
func boolPtr(b bool) *bool { return &b }

// legacyPreseedUsers is the FIFO queue used by tests that pre-date the
// namespaces work. The order (admin first, custom second) is load-bearing:
// existing tests pull the admin token then the custom token by FIFO order
// and depend on this contract.
var legacyPreseedUsers = []mockoidc.User{
	&mockoidc.MockUser{Subject: "admin-user"},
	&mockoidc.MockUser{Subject: "custom-user", Groups: []string{"custom-group"}},
}

// namespacePreseedUsers contains only OIDC users that pass classification:
// valid namespaced principals (assuming the cluster has those namespaces)
// and an explicit global operator. Tests that just need to act as a
// namespaced or global user pull from here.
var namespacePreseedUsers = []mockoidc.User{
	&namespacedUser{
		MockUser:  mockoidc.MockUser{Subject: "oidc-namespaced-customer1"},
		Namespace: "customer1",
	},
	&namespacedUser{
		MockUser:  mockoidc.MockUser{Subject: "oidc-namespaced-customer2"},
		Namespace: "customer2",
	},
	&namespacedUser{
		MockUser:        mockoidc.MockUser{Subject: "oidc-global"},
		GlobalPrincipal: boolPtr(true),
	},
	// Member of the AllUsers OIDC group; used for testing group-based
	// role assignments. The cluster matcher specializes a group-bound
	// role to the principal's namespace at enforce time, so this user
	// inherits whatever role is assigned to "AllUsers" inside customer1.
	&namespacedUser{
		MockUser:  mockoidc.MockUser{Subject: "oidc-customer1-group-member", Groups: []string{"AllUsers"}},
		Namespace: "customer1",
	},
	// A neutral subject reused across tenants. Tests pair this with a
	// DB user of the same short id to prove that the qualified storage
	// path keeps tenants' identities distinct.
	&namespacedUser{
		MockUser:  mockoidc.MockUser{Subject: "user"},
		Namespace: "customer1",
	},
}

// pickPreseedUsers selects the active preseed list from the
// MOCK_OIDC_PRESEED env var.
func pickPreseedUsers() []mockoidc.User {
	switch os.Getenv("MOCK_OIDC_PRESEED") {
	case "namespaces":
		return namespacePreseedUsers
	default:
		return legacyPreseedUsers
	}
}

// userBySubject indexes the active preseed list by Subject for the /queue
// admin endpoint.
func userBySubject(users []mockoidc.User) map[string]mockoidc.User {
	out := make(map[string]mockoidc.User, len(users))
	for _, u := range users {
		out[u.ID()] = u
	}
	return out
}

// queueUser appends a user + matching auth code to the FIFO queues.
func queueUser(m *mockoidc.MockOIDC, u mockoidc.User) {
	m.QueueUser(u)
	m.QueueCode(authCode)
}

// adminQueueHandler drains and replaces the user/code queues with the
// requested subject. The replace-not-append behaviour keeps the operation
// order-independent: any pre-seeded entry from FIFO drift gets discarded,
// and the next /tokens call dequeues exactly the requested user.
//
// Not safe for parallel use: two callers racing /queue + /tokens against
// the same mock instance can interleave and dequeue the wrong user.
// Acceptance tests that pin a subject must serialize the queue+token
// pair (no t.Parallel between subtests sharing the mock).
func adminQueueHandler(m *mockoidc.MockOIDC, index map[string]mockoidc.User) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		subject := r.URL.Query().Get("subject")
		if subject == "" {
			http.Error(w, "missing subject query parameter", http.StatusBadRequest)
			return
		}
		user, ok := index[subject]
		if !ok {
			http.Error(w, fmt.Sprintf("unknown subject %q", subject), http.StatusNotFound)
			return
		}

		m.UserQueue.Lock()
		m.UserQueue.Queue = []mockoidc.User{user}
		m.UserQueue.Unlock()

		m.SessionStore.CodeQueue.Lock()
		m.SessionStore.CodeQueue.Queue = []string{authCode}
		m.SessionStore.CodeQueue.Unlock()

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"queued": subject})
	}
}

func main() {
	getTLSConfig := func() *tls.Config {
		certificate := os.Getenv("MOCK_CERTIFICATE")
		certificateKey := os.Getenv("MOCK_CERTIFICATE_PRIVATE_KEY")
		if certificate != "" && certificateKey != "" {
			log.Println("Creating TLS config self signed certificates")
			// read certificates
			certBlock, _ := pem.Decode([]byte(certificate))
			cert, err := x509.ParseCertificate(certBlock.Bytes)
			if err != nil {
				log.Fatalf("parse certificate: %v", err)
			}
			var privKey crypto.PrivateKey
			keyBlock, _ := pem.Decode([]byte(certificateKey))
			if keyBlock.Type == "RSA PRIVATE KEY" {
				privKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
				if err != nil {
					log.Fatalf("parse certificate's private rsa key: %v", err)
				}
			} else {
				privKey, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
				if err != nil {
					log.Fatalf("parse certificate's private key: %v", err)
				}
			}
			// TLS configuration
			tlsCert := tls.Certificate{
				Certificate: [][]byte{cert.Raw},
				PrivateKey:  privKey,
			}
			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{tlsCert},
				MinVersion:   tls.VersionTLS12,
			}
			return tlsConfig
		}
		// Default OIDC server
		return nil
	}
	tlsConfig := getTLSConfig()
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	m, _ := mockoidc.NewServer(rsaKey)
	m.ClientSecret = clientSecret
	m.ClientID = clientID
	hostname := os.Getenv("MOCK_HOSTNAME")
	m.Hostname = fmt.Sprintf("%s:48001", hostname)
	addr := "0.0.0.0:48001"
	if hostname != "" {
		addr = fmt.Sprintf("%s:48001", hostname)
	}
	ln, _ := net.Listen("tcp", addr)
	m.Start(ln, tlsConfig)
	defer m.Shutdown()

	preseedUsers := pickPreseedUsers()
	for _, u := range preseedUsers {
		queueUser(m, u)
	}

	// Admin server on :48002 — exposes POST /queue?subject=<name>.
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/queue", adminQueueHandler(m, userBySubject(preseedUsers)))
	adminAddr := "0.0.0.0:48002"
	if hostname != "" {
		adminAddr = fmt.Sprintf("%s:48002", hostname)
	}
	adminServer := &http.Server{Addr: adminAddr, Handler: adminMux}
	go func() {
		log.Printf("admin endpoint listening on %s", adminAddr)
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("admin server failed: %v", err)
		}
	}()
	defer adminServer.Close()

	log.Printf("issuer: %v\n", m.Issuer())
	log.Printf("discovery endpoint: %v\n", m.DiscoveryEndpoint())
	// Create a channel to receive OS signals
	sigChan := make(chan os.Signal, 1)
	// Notify the channel of the interrupt signal (Ctrl+C)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Print a message indicating the program is running
	log.Println("Program is running. Press Ctrl+C to stop.")

	// Block until a signal is received
	sig := <-sigChan
	log.Printf("Received signal: %v. Shutting down...\n", sig)
}
