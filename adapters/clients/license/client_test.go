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
