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
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
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
	id, priv, err := ParseKey(key)
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

// TestParseKeyErrors is the single form-check table for the key format; the
// startup gate in usecases/config delegates to ParseKey, so these cases cover
// both. Whitespace, padding and non-canonical base64 are all rejected.
func TestParseKeyErrors(t *testing.T) {
	l, _ := Generate()
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	validSeed := base64.RawURLEncoding.EncodeToString(seed)
	valid := "wv8." + l.ID + "." + validSeed

	cases := []struct {
		name string
		key  string
		want error
	}{
		{"empty", "", ErrMalformedKey},
		{"no separators", "wv8lic_01ARZ3NDEKTSV4RRFFQ69G5FAVAAAA", ErrMalformedKey},
		{"too many segments", valid + ".extra", ErrMalformedKey},
		{"missing seed segment", "wv8." + l.ID, ErrMalformedKey},
		{"wrong format prefix", "wv7." + l.ID + "." + validSeed, ErrBadPrefix},
		{"uppercase format prefix", "WV8." + l.ID + "." + validSeed, ErrBadPrefix},
		{"leading space", " " + valid, ErrBadPrefix},
		{"trailing newline", valid + "\n", ErrMalformedKey},
		{"trailing space", valid + " ", ErrMalformedKey},
		{"wrong id prefix", "wv8.foo_01ARZ3NDEKTSV4RRFFQ69G5FAV." + validSeed, ErrBadID},
		{"id too short", "wv8.lic_short.abc", ErrBadID},
		{"id too long", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAVV." + validSeed, ErrBadID},
		{"id with excluded char I", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAI." + validSeed, ErrBadID},
		{"id with excluded char L", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAL." + validSeed, ErrBadID},
		{"id with excluded char O", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAO." + validSeed, ErrBadID},
		{"id with excluded char U", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAU." + validSeed, ErrBadID},
		{"id with lowercase char", "wv8.lic_01ARZ3NDEKTSV4RRFFQ69G5FAa." + validSeed, ErrBadID},
		{"seed not base64", "wv8." + l.ID + ".not-base64!!", ErrMalformedKey},
		{"seed too short", "wv8." + l.ID + ".AAAA", ErrMalformedKey},
		{"seed too long", "wv8." + l.ID + "." + validSeed + "AA", ErrMalformedKey},
		{"seed with padding", "wv8." + l.ID + "." + validSeed[:42] + "=", ErrMalformedKey},
		{"seed with standard base64 char", "wv8." + l.ID + "." + strings.Replace(validSeed, "A", "+", 1), ErrMalformedKey},
		// Decodes to the same 32 bytes as 43x"A", but the encoding is not
		// canonical (non-zero trailing bits), so it must be rejected.
		{"seed with non-zero trailing bits", "wv8." + l.ID + "." + strings.Repeat("A", 42) + "B", ErrMalformedKey},
		// DecodeString ignores embedded CR/LF; the canonical re-encode check
		// must reject these.
		{"seed with embedded newline", "wv8." + l.ID + "." + strings.Replace(validSeed, "AA", "A\nA", 1), ErrMalformedKey},
		{"seed with embedded carriage return", "wv8." + l.ID + "." + strings.Replace(validSeed, "AA", "A\rA", 1), ErrMalformedKey},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := ParseKey(tt.key)
			if !errors.Is(err, tt.want) {
				t.Errorf("ParseKey(%q) = %v, want %v", tt.key, err, tt.want)
			}
		})
	}
}

func TestIDsSortByTime(t *testing.T) {
	a, _ := NewID()
	time.Sleep(2 * time.Millisecond)
	b, _ := NewID()
	if a >= b {
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

func TestCheckFreshnessRejectsExtremeTimestamps(t *testing.T) {
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	// Far enough in the future, now.Sub(timestamp) saturates and negating
	// the duration overflows — the request must still be rejected.
	farFuture := VerifyRequest{Timestamp: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := farFuture.CheckFreshness(now); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("far-future request accepted: %v", err)
	}
	farPast := VerifyRequest{Timestamp: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := farPast.CheckFreshness(now); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("far-past request accepted: %v", err)
	}
}

func TestServerKeySetVerifyRejectsWrongLengthKey(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	srv := ServerKey{ID: "srv", PrivateKey: priv}
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
	// ed25519.Verify panics on a wrong-length key; a truncated trusted key
	// must produce an error instead.
	truncated := ServerKeySet{"srv": pub[:ed25519.PublicKeySize-1]}
	if err := truncated.Verify(resp); !errors.Is(err, ErrUnknownServerKey) {
		t.Fatalf("wrong-length trusted key: %v", err)
	}
}
