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

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/license"
)

// resolveLicenseState resolves the license key configured via LICENSE_KEY or
// LICENSE_KEY_FILE into the license state. All license key handling lives in
// this one method; the caller just assigns the result.
//
// Setting both variables, or pointing LICENSE_KEY_FILE at an unreadable file,
// is a startup error: those are clear misconfigurations that should fail fast
// rather than silently fall back to free mode. An unset, empty or malformed
// key is not an error — it simply runs in free mode with a warning. The key
// itself is never logged; only the non-secret license id is kept.
func resolveLicenseState() (license.State, error) {
	key, keyFile := os.Getenv("LICENSE_KEY"), os.Getenv("LICENSE_KEY_FILE")
	if key != "" && keyFile != "" {
		return license.State{}, fmt.Errorf("LICENSE_KEY and LICENSE_KEY_FILE are mutually exclusive; set only one")
	}
	if keyFile != "" {
		contents, err := os.ReadFile(keyFile)
		if err != nil {
			return license.State{}, fmt.Errorf("cannot read LICENSE_KEY_FILE: %w", err)
		}
		key = strings.TrimSpace(string(contents))
	}

	switch {
	case key == "" && keyFile != "":
		logrus.Warn("LICENSE_KEY_FILE is set but the file contains no key; " +
			"Weaviate-licensed functionality is disabled")
	case key != "" && !licenseKeyWellFormed(key):
		logrus.Warn("the license key configured via LICENSE_KEY or LICENSE_KEY_FILE is not a " +
			"well-formed Weaviate license key; Weaviate-licensed functionality is disabled")
	}

	if !licenseKeyWellFormed(key) {
		return license.State{Status: license.StatusUnlicensed}, nil
	}
	return license.State{Status: license.StatusValid, LicenseID: licenseID(key)}, nil
}

// licenseID extracts the non-secret license id from a well-formed key.
func licenseID(key string) string {
	return strings.Split(key, ".")[1]
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
