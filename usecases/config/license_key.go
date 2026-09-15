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

package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strings"
)

// licenseKeyFromEnv returns the license key configured via LICENSE_KEY or
// LICENSE_KEY_FILE. The two are mutually exclusive, and an unreadable file is
// a startup error: both are clear misconfigurations that should fail fast
// rather than silently fall back to free mode.
func licenseKeyFromEnv() (string, error) {
	key, keyFile := os.Getenv("LICENSE_KEY"), os.Getenv("LICENSE_KEY_FILE")
	if key != "" && keyFile != "" {
		return "", fmt.Errorf("LICENSE_KEY and LICENSE_KEY_FILE are mutually exclusive; set only one")
	}
	if keyFile == "" {
		return key, nil
	}
	contents, err := os.ReadFile(keyFile)
	if err != nil {
		return "", fmt.Errorf("cannot read LICENSE_KEY_FILE: %w", err)
	}
	return strings.TrimSpace(string(contents)), nil
}

// licenseKeyWellFormed reports whether key has the form of a Weaviate
// license key: "wv8.<license_id>.<seed>", where license_id is "lic_"
// followed by 26 Crockford base32 characters (ULID layout) and seed is the
// base64url encoding (no padding) of a 32-byte ed25519 seed.
//
// It validates the form only. A well-formed key is not necessarily a valid
// license.
func licenseKeyWellFormed(key string) bool {
	parts := strings.Split(key, ".")
	if len(parts) != 3 || parts[0] != "wv8" {
		return false
	}
	if !licenseIDWellFormed(parts[1]) {
		return false
	}
	seed, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(seed) != 32 {
		return false
	}
	// Require the canonical encoding: DecodeString alone ignores CR/LF and
	// accepts non-zero trailing bits.
	return base64.RawURLEncoding.EncodeToString(seed) == parts[2]
}

func licenseIDWellFormed(id string) bool {
	const prefix = "lic_"
	if len(id) != len(prefix)+26 || !strings.HasPrefix(id, prefix) {
		return false
	}
	for _, c := range id[len(prefix):] {
		if !isCrockfordBase32(c) {
			return false
		}
	}
	return true
}

func isCrockfordBase32(c rune) bool {
	switch {
	case c >= '0' && c <= '9':
		return true
	case c >= 'A' && c <= 'Z':
		// Crockford base32 excludes I, L, O and U
		return c != 'I' && c != 'L' && c != 'O' && c != 'U'
	}
	return false
}
