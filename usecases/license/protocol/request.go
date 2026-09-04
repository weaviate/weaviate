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
	"encoding/base64"
	"errors"
	"time"
)

// NonceSize is the number of random bytes in a request nonce.
const NonceSize = 16

// MaxClockSkew is how far a request timestamp may be from server time.
const MaxClockSkew = 5 * time.Minute

var (
	ErrBadSignature = errors.New("license: signature verification failed")
	ErrStaleRequest = errors.New("license: request timestamp outside allowed window")
	ErrMissingField = errors.New("license: required field missing")
)

// VerifyRequest is the challenge a Weaviate instance sends to the license
// service. Signature is an ed25519 signature, made with the customer's private
// key, over the canonical JSON of every other field.
type VerifyRequest struct {
	LicenseID       string    `json:"license_id"`
	ClusterID       string    `json:"cluster_id"`
	InstanceID      string    `json:"instance_id"`
	WeaviateVersion string    `json:"weaviate_version"`
	Timestamp       time.Time `json:"timestamp"`
	Nonce           string    `json:"nonce"`
	Signature       string    `json:"signature,omitempty"`
}

// NewNonce returns a fresh base64url nonce.
func NewNonce() (string, error) {
	var b [NonceSize]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// payload is the signed portion of a request. Timestamp is fixed to RFC3339
// in UTC at second precision so both sides canonicalise identically.
func (r VerifyRequest) payload() ([]byte, error) {
	if r.LicenseID == "" || r.InstanceID == "" || r.Nonce == "" || r.Timestamp.IsZero() {
		return nil, ErrMissingField
	}
	return Canonical(map[string]string{
		"license_id":       r.LicenseID,
		"cluster_id":       r.ClusterID,
		"instance_id":      r.InstanceID,
		"weaviate_version": r.WeaviateVersion,
		"timestamp":        r.Timestamp.UTC().Format(time.RFC3339),
		"nonce":            r.Nonce,
	})
}

// Sign fills in Timestamp (if zero) and Nonce (if empty), normalises the
// timestamp to UTC seconds, and sets Signature.
func (r *VerifyRequest) Sign(priv ed25519.PrivateKey) error {
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}
	r.Timestamp = r.Timestamp.UTC().Truncate(time.Second)
	if r.Nonce == "" {
		n, err := NewNonce()
		if err != nil {
			return err
		}
		r.Nonce = n
	}
	p, err := r.payload()
	if err != nil {
		return err
	}
	r.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, p))
	return nil
}

// VerifySignature checks Signature against pub. It does not check freshness.
func (r VerifyRequest) VerifySignature(pub ed25519.PublicKey) error {
	p, err := r.payload()
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.DecodeString(r.Signature)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return ErrBadSignature
	}
	if !ed25519.Verify(pub, p, sig) {
		return ErrBadSignature
	}
	return nil
}

// CheckFreshness returns ErrStaleRequest if the request timestamp is more
// than MaxClockSkew away from now.
func (r VerifyRequest) CheckFreshness(now time.Time) error {
	d := now.Sub(r.Timestamp)
	if d < 0 {
		d = -d
	}
	if d > MaxClockSkew {
		return ErrStaleRequest
	}
	return nil
}
