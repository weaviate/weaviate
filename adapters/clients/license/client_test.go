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

package license

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/weaviate/weaviate/entities/license"
)

func TestVerifyToleratesTrailingSlashServerURL(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	key := license.ServerKey{ID: "k", PrivateKey: priv}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/verify" {
			t.Errorf("unexpected request path %q", r.URL.Path)
		}
		var req license.VerifyRequest
		json.NewDecoder(r.Body).Decode(&req)
		now := time.Now().UTC().Truncate(time.Second)
		resp := license.VerifyResponse{
			LicenseID: req.LicenseID, Status: license.StatusValid,
			ExpiresAt: now.Add(time.Hour), CheckedAt: now, NextCheckAfter: now.Add(24 * time.Hour), Nonce: req.Nonce,
		}
		key.Sign(&resp)
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	lic, _ := license.Generate()
	client, err := NewClient(lic.Key(), license.ServerKeySet{"k": pub})
	if err != nil {
		t.Fatal(err)
	}
	client.ServerURL = srv.URL + "/"
	resp, err := client.Verify(context.Background(), "c-1", "n-1", "1.39.0")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != license.StatusValid {
		t.Fatalf("unexpected status %q", resp.Status)
	}
}

// TestVerifyTrustChecks covers the two checks that make an answer
// trustworthy: the server signature against the trusted key set, and the
// echo of this request's license ID and nonce. Every bad row must produce an
// error and a zero response; each row fails if its corresponding check is
// removed.
func TestVerifyTrustChecks(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	_, roguePriv, _ := ed25519.GenerateKey(rand.Reader)

	lic, err := license.Generate()
	if err != nil {
		t.Fatal(err)
	}
	trusted := license.ServerKeySet{"k": pub}

	cases := []struct {
		name             string
		signKey          ed25519.PrivateKey
		mutateBeforeSign func(*license.VerifyResponse) // signature stays valid
		mutateAfterSign  func(*license.VerifyResponse) // breaks the signature
		wantErr          bool
	}{
		{"valid answer", priv, nil, nil, false},
		{"signed by an untrusted key", roguePriv, nil, nil, true},
		{"different nonce, valid signature", priv, func(r *license.VerifyResponse) { r.Nonce = "other-nonce" }, nil, true},
		{"different license id, valid signature", priv, func(r *license.VerifyResponse) { r.LicenseID = "lic_01ARZ3NDEKTSV4RRFFQ69G5FAV" }, nil, true},
		{"field tampered after signing", priv, nil, func(r *license.VerifyResponse) { r.Status = license.StatusRevoked }, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req license.VerifyRequest
				json.NewDecoder(r.Body).Decode(&req)
				now := time.Now().UTC().Truncate(time.Second)
				resp := license.VerifyResponse{
					LicenseID: req.LicenseID, Status: license.StatusValid,
					ExpiresAt: now.Add(time.Hour), CheckedAt: now, NextCheckAfter: now.Add(24 * time.Hour), Nonce: req.Nonce,
				}
				if tt.mutateBeforeSign != nil {
					tt.mutateBeforeSign(&resp)
				}
				key := license.ServerKey{ID: "k", PrivateKey: tt.signKey}
				key.Sign(&resp)
				if tt.mutateAfterSign != nil {
					tt.mutateAfterSign(&resp)
				}
				json.NewEncoder(w).Encode(resp)
			}))
			defer srv.Close()

			client, err := NewClient(lic.Key(), trusted)
			if err != nil {
				t.Fatal(err)
			}
			client.ServerURL = srv.URL
			resp, err := client.Verify(context.Background(), "c-1", "n-1", "1.39.0")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				if resp != (license.VerifyResponse{}) {
					t.Fatalf("expected a zero response on error, got %+v", resp)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if resp.Status != license.StatusValid {
				t.Fatalf("unexpected status %q", resp.Status)
			}
		})
	}
}
