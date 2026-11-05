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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"
)

// Status is the outcome of a verify call.
type Status string

const (
	StatusValid   Status = "valid"
	StatusExpired Status = "expired"
	StatusRevoked Status = "revoked"
	StatusUnknown Status = "unknown"

	EntitlementEnterprise = "edition:enterprise"
	EntitlementPlatform   = "edition:platform"

	// ProtocolVersion is sent as the X-License-Protocol response header, not
	// inside the signed payload.
	ProtocolVersion = 1
)

var ErrUnknownServerKey = errors.New("license: response signed by unknown server key")

// KeyIDFor derives a server key id from its public key, so an id can never
// name a different key than the one it was derived from.
func KeyIDFor(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return "k" + base64.RawURLEncoding.EncodeToString(sum[:8])
}

// ServerSig is an additional signature over the same payload by another
// active server key.
type ServerSig struct {
	KeyID     string `json:"key_id"`
	Signature string `json:"signature"`
}

// VerifyResponse is the license service's answer. ServerSignature is an
// ed25519 signature by the service's key (identified by ServerKeyID) over the
// canonical JSON of the protocol fields, excluding both signature fields, so
// a client with the embedded server public key can reject forged answers.
type VerifyResponse struct {
	LicenseID       string    `json:"license_id"`
	Status          Status    `json:"status"`
	ExpiresAt       time.Time `json:"expires_at"`
	CheckedAt       time.Time `json:"checked_at"`
	NextCheckAfter  time.Time `json:"next_check_after"`
	Nonce           string    `json:"nonce"`
	ClusterMismatch bool      `json:"cluster_mismatch,omitempty"`
	ServerKeyID     string    `json:"server_key_id"`
	ServerSignature string    `json:"server_signature,omitempty"`
	// A nil Entitlements slice represents a legacy response without the
	// field. Non-nil slices, including empty ones, are signed.
	Entitlements []string `json:"entitlements"`
	// ServerSignatures carries signatures by the other active keys over the
	// same payload. Each entry is checked with its own key.
	ServerSignatures []ServerSig `json:"server_signatures,omitempty"`
}

func (r VerifyResponse) payload() ([]byte, error) {
	fields := map[string]any{
		"license_id":       r.LicenseID,
		"status":           string(r.Status),
		"expires_at":       r.ExpiresAt.UTC().Format(time.RFC3339),
		"checked_at":       r.CheckedAt.UTC().Format(time.RFC3339),
		"next_check_after": r.NextCheckAfter.UTC().Format(time.RFC3339),
		"nonce":            r.Nonce,
		"cluster_mismatch": r.ClusterMismatch,
		"server_key_id":    r.ServerKeyID,
	}
	if r.Entitlements != nil {
		fields["entitlements"] = r.Entitlements
	}
	return Canonical(fields)
}

// EffectiveEntitlements applies the backward-compatible default for a legacy
// response that predates entitlements.
func (r VerifyResponse) EffectiveEntitlements() []string {
	if r.Entitlements == nil {
		return []string{EntitlementEnterprise}
	}
	return r.Entitlements
}

// ServerKey is the license service's signing key.
type ServerKey struct {
	ID         string
	PrivateKey ed25519.PrivateKey
}

// Sign normalises timestamps to UTC seconds and sets ServerKeyID and
// ServerSignature.
func (k ServerKey) Sign(r *VerifyResponse) error {
	r.ExpiresAt = r.ExpiresAt.UTC().Truncate(time.Second)
	r.CheckedAt = r.CheckedAt.UTC().Truncate(time.Second)
	r.NextCheckAfter = r.NextCheckAfter.UTC().Truncate(time.Second)
	r.ServerKeyID = k.ID
	p, err := r.payload()
	if err != nil {
		return err
	}
	r.ServerSignature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(k.PrivateKey, p))
	r.ServerSignatures = nil
	return nil
}

// SignWith signs with the primary key and adds a signature by every extra
// key, so a client that trusts any one of them accepts the answer.
func SignWith(r *VerifyResponse, primary ServerKey, extra ...ServerKey) error {
	if err := primary.Sign(r); err != nil {
		return err
	}
	if len(extra) == 0 {
		return nil
	}
	p, err := r.payload()
	if err != nil {
		return err
	}
	for _, k := range extra {
		if k.ID == primary.ID {
			continue
		}
		r.ServerSignatures = append(r.ServerSignatures, ServerSig{
			KeyID:     k.ID,
			Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(k.PrivateKey, p)),
		})
	}
	return nil
}

// ServerKeySet is the set of server public keys a client trusts, keyed by ID.
// Weaviate embeds the current and previous key so rotation does not break
// older binaries.
type ServerKeySet map[string]ed25519.PublicKey

// Verify checks the response signature against the key named by ServerKeyID,
// or, when that key is not trusted, against an additional trusted signature.
func (s ServerKeySet) Verify(r VerifyResponse) error {
	p, err := r.payload()
	if err != nil {
		return err
	}
	if pub, ok := s[r.ServerKeyID]; ok {
		return verifySig(pub, p, r.ServerSignature)
	}
	for _, alt := range r.ServerSignatures {
		if pub, ok := s[alt.KeyID]; ok {
			return verifySig(pub, p, alt.Signature)
		}
	}
	return ErrUnknownServerKey
}

func verifySig(pub ed25519.PublicKey, payload []byte, sigB64 string) error {
	if len(pub) != ed25519.PublicKeySize {
		return ErrUnknownServerKey
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(pub, payload, sig) {
		return ErrBadSignature
	}
	return nil
}

// Matches reports whether the response answers the given request: same
// license ID and nonce.
func (r VerifyResponse) Matches(req VerifyRequest) bool {
	return r.LicenseID == req.LicenseID && r.Nonce == req.Nonce
}
