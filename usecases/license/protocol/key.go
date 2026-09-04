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
	"encoding/base32"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// KeyPrefix is the customer-facing license key format marker ("weavi-8").
const KeyPrefix = "wv8"

// IDPrefix is the prefix of every license ID.
const IDPrefix = "lic_"

// DefaultTerm is the default license validity period.
const DefaultTerm = 365 * 24 * time.Hour

var (
	ErrMalformedKey = errors.New("license: malformed license key")
	ErrBadPrefix    = errors.New("license: unsupported license key prefix")
	ErrBadID        = errors.New("license: malformed license id")
)

// License is a freshly generated license: the ID and ed25519 key pair.
// The private key is handed to the customer; the service keeps the public key
// and, optionally, an encrypted copy of the seed.
type License struct {
	ID         string
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
}

// Key returns the customer-facing license key string.
func (l License) Key() string {
	return FormatKey(l.ID, l.PrivateKey)
}

// Generate creates a new license ID and ed25519 key pair.
func Generate() (License, error) {
	id, err := NewID()
	if err != nil {
		return License{}, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return License{}, fmt.Errorf("license: generate key: %w", err)
	}
	return License{ID: id, PublicKey: pub, PrivateKey: priv}, nil
}

// crockford is the Crockford base32 alphabet used for IDs (no I, L, O, U).
var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

// NewID returns a new license ID: "lic_" followed by 26 Crockford base32
// characters encoding 48 bits of millisecond timestamp and 80 bits of
// randomness (ULID layout). IDs sort by creation time.
func NewID() (string, error) {
	var b [16]byte
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	if _, err := rand.Read(b[6:]); err != nil {
		return "", fmt.Errorf("license: generate id: %w", err)
	}
	return IDPrefix + crockford.EncodeToString(b[:]), nil
}

// ValidID reports whether s has the shape of a license ID.
func ValidID(s string) bool {
	if !strings.HasPrefix(s, IDPrefix) {
		return false
	}
	rest := s[len(IDPrefix):]
	if len(rest) != 26 {
		return false
	}
	_, err := crockford.DecodeString(rest)
	return err == nil
}

// FormatKey builds the customer-facing key: wv8.<license_id>.<base64url(seed)>.
func FormatKey(id string, priv ed25519.PrivateKey) string {
	seed := priv.Seed()
	return KeyPrefix + "." + id + "." + base64.RawURLEncoding.EncodeToString(seed)
}

// ParseKey parses a customer-facing key back into its license ID and private key.
func ParseKey(key string) (id string, priv ed25519.PrivateKey, err error) {
	key = strings.TrimSpace(key)
	parts := strings.Split(key, ".")
	if len(parts) != 3 {
		return "", nil, ErrMalformedKey
	}
	if parts[0] != KeyPrefix {
		return "", nil, ErrBadPrefix
	}
	if !ValidID(parts[1]) {
		return "", nil, ErrBadID
	}
	seed, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(seed) != ed25519.SeedSize {
		return "", nil, ErrMalformedKey
	}
	return parts[1], ed25519.NewKeyFromSeed(seed), nil
}

// FormatKeyFromSeed builds the customer-facing key from a stored 32-byte seed.
func FormatKeyFromSeed(id string, seed []byte) (string, error) {
	if len(seed) != ed25519.SeedSize {
		return "", ErrMalformedKey
	}
	if !ValidID(id) {
		return "", ErrBadID
	}
	return FormatKey(id, ed25519.NewKeyFromSeed(seed)), nil
}
