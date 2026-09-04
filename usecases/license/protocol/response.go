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
)

var ErrUnknownServerKey = errors.New("license: response signed by unknown server key")

// VerifyResponse is the license service's answer. ServerSignature is an
// ed25519 signature by the service's key (identified by ServerKeyID) over the
// canonical JSON of every other field, so a client with the embedded server
// public key can reject forged answers.
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
}

func (r VerifyResponse) payload() ([]byte, error) {
	return Canonical(map[string]any{
		"license_id":       r.LicenseID,
		"status":           string(r.Status),
		"expires_at":       r.ExpiresAt.UTC().Format(time.RFC3339),
		"checked_at":       r.CheckedAt.UTC().Format(time.RFC3339),
		"next_check_after": r.NextCheckAfter.UTC().Format(time.RFC3339),
		"nonce":            r.Nonce,
		"cluster_mismatch": r.ClusterMismatch,
		"server_key_id":    r.ServerKeyID,
	})
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
	return nil
}

// ServerKeySet is the set of server public keys a client trusts, keyed by ID.
// Weaviate embeds the current and previous key so rotation does not break
// older binaries.
type ServerKeySet map[string]ed25519.PublicKey

// Verify checks the response signature against the key named by ServerKeyID.
func (s ServerKeySet) Verify(r VerifyResponse) error {
	pub, ok := s[r.ServerKeyID]
	if !ok {
		return ErrUnknownServerKey
	}
	p, err := r.payload()
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.DecodeString(r.ServerSignature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return ErrBadSignature
	}
	if !ed25519.Verify(pub, p, sig) {
		return ErrBadSignature
	}
	return nil
}

// Matches reports whether the response answers the given request: same
// license ID and nonce.
func (r VerifyResponse) Matches(req VerifyRequest) bool {
	return r.LicenseID == req.LicenseID && r.Nonce == req.Nonce
}
