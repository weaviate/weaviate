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

package protocol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestGenerateAndKeyRoundTrip(t *testing.T) {
	l, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidID(l.ID) {
		t.Fatalf("invalid id %q", l.ID)
	}
	key := l.Key()
	if !strings.HasPrefix(key, "wv8."+l.ID+".") {
		t.Fatalf("unexpected key shape %q", key)
	}
	id, priv, err := ParseKey("  " + key + "\n")
	if err != nil {
		t.Fatal(err)
	}
	if id != l.ID {
		t.Fatalf("id mismatch: %q vs %q", id, l.ID)
	}
	if !priv.Equal(l.PrivateKey) {
		t.Fatal("private key did not round-trip")
	}
	if !priv.Public().(ed25519.PublicKey).Equal(l.PublicKey) {
		t.Fatal("public key mismatch")
	}
}

func TestParseKeyErrors(t *testing.T) {
	l, _ := Generate()
	cases := map[string]error{
		"":                              ErrMalformedKey,
		"wv8." + l.ID:                   ErrMalformedKey,
		"wv1." + l.ID + ".abc":          ErrBadPrefix,
		"wv8.lic_short.abc":             ErrBadID,
		"wv8." + l.ID + ".not-base64!!": ErrMalformedKey,
		"wv8." + l.ID + ".AAAA":         ErrMalformedKey, // wrong seed length
	}
	for in, want := range cases {
		_, _, err := ParseKey(in)
		if !errors.Is(err, want) {
			t.Errorf("ParseKey(%q) = %v, want %v", in, err, want)
		}
	}
}

func TestIDsSortByTime(t *testing.T) {
	a, _ := NewID()
	time.Sleep(2 * time.Millisecond)
	b, _ := NewID()
	if !(a < b) {
		t.Fatalf("ids not time-ordered: %s >= %s", a, b)
	}
}

func TestCanonicalIsDeterministic(t *testing.T) {
	a, _ := Canonical(map[string]any{"b": 1, "a": "x<y>&", "c": []int{1, 2}})
	b, _ := Canonical(struct {
		C []int  `json:"c"`
		A string `json:"a"`
		B int    `json:"b"`
	}{[]int{1, 2}, "x<y>&", 1})
	want := `{"a":"x<y>&","b":1,"c":[1,2]}`
	if string(a) != want || string(b) != want {
		t.Fatalf("canonical mismatch:\n a=%s\n b=%s\n want=%s", a, b, want)
	}
}

func TestRequestSignAndVerify(t *testing.T) {
	l, _ := Generate()
	req := VerifyRequest{
		LicenseID:       l.ID,
		ClusterID:       "c-1",
		InstanceID:      "node-a",
		WeaviateVersion: "1.34.2",
		Timestamp:       time.Date(2026, 9, 4, 10, 0, 0, 123456789, time.FixedZone("CEST", 2*3600)),
	}
	if err := req.Sign(l.PrivateKey); err != nil {
		t.Fatal(err)
	}
	if req.Nonce == "" || req.Signature == "" {
		t.Fatal("nonce or signature not set")
	}
	if req.Timestamp.Location() != time.UTC || req.Timestamp.Nanosecond() != 0 {
		t.Fatalf("timestamp not normalised: %v", req.Timestamp)
	}

	// Simulate the wire: encode to JSON and decode on the server side.
	wire, _ := json.Marshal(req)
	var got VerifyRequest
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if err := got.VerifySignature(l.PublicKey); err != nil {
		t.Fatalf("verify after wire round-trip: %v", err)
	}
	if err := got.CheckFreshness(req.Timestamp.Add(MaxClockSkew - time.Second)); err != nil {
		t.Fatalf("fresh request rejected: %v", err)
	}
	if err := got.CheckFreshness(req.Timestamp.Add(MaxClockSkew + time.Second)); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("stale request accepted: %v", err)
	}

	// Tampering with any signed field must fail.
	tampered := got
	tampered.ClusterID = "c-2"
	if err := tampered.VerifySignature(l.PublicKey); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered cluster_id accepted: %v", err)
	}
	// A different customer's key must fail.
	other, _ := Generate()
	if err := got.VerifySignature(other.PublicKey); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("wrong key accepted: %v", err)
	}
	// Missing fields are rejected before signing.
	var empty VerifyRequest
	if err := empty.Sign(l.PrivateKey); !errors.Is(err, ErrMissingField) {
		t.Fatalf("empty request signed: %v", err)
	}
}

func TestResponseSignAndVerify(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	srv := ServerKey{ID: "srv-2026-09", PrivateKey: priv}
	trusted := ServerKeySet{"srv-2026-09": pub}

	now := time.Now()
	resp := VerifyResponse{
		LicenseID:      "lic_01J9ABCDEFGHJKMNPQRSTVWXYZ",
		Status:         StatusValid,
		ExpiresAt:      now.Add(DefaultTerm),
		CheckedAt:      now,
		NextCheckAfter: now.Add(24 * time.Hour),
		Nonce:          "n1",
	}
	if err := srv.Sign(&resp); err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(resp)
	var got VerifyResponse
	if err := json.Unmarshal(wire, &got); err != nil {
		t.Fatal(err)
	}
	if err := trusted.Verify(got); err != nil {
		t.Fatalf("verify after wire round-trip: %v", err)
	}
	if !got.Matches(VerifyRequest{LicenseID: resp.LicenseID, Nonce: "n1"}) {
		t.Fatal("response should match its request")
	}

	// Forging "valid" over a revoked answer must fail.
	forged := got
	forged.Status = StatusRevoked
	if err := trusted.Verify(forged); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("forged status accepted: %v", err)
	}
	// Flipping cluster_mismatch is covered by the signature too.
	forged = got
	forged.ClusterMismatch = true
	if err := trusted.Verify(forged); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("forged cluster_mismatch accepted: %v", err)
	}
	// Unknown key id is rejected before signature check.
	forged = got
	forged.ServerKeyID = "srv-9999"
	if err := trusted.Verify(forged); !errors.Is(err, ErrUnknownServerKey) {
		t.Fatalf("unknown key accepted: %v", err)
	}
	// A rotated-out key that the client no longer trusts is rejected.
	if err := (ServerKeySet{}).Verify(got); !errors.Is(err, ErrUnknownServerKey) {
		t.Fatalf("empty key set accepted: %v", err)
	}
}
