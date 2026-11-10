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
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	protocol "github.com/weaviate/weaviate/entities/license"
)

// productionServerKeys is a string so deliberately relinked test binaries can
// use a local signing key. Normal binaries use these public keys as compiled.
var productionServerKeys = "kSqW93_D-AqI:QtbmlPpmSqodZMBBP2nNyjNyGRNjz3Ejm_KXtNrMVFs," +
	"srv-2026-09:xntuOBLp6UT9ek7dmbO6fpRscU40FCccICGO9ftEZ2Q"

// TrustedServerKeys returns the public keys embedded in the binary. The binary
// never fetches trust anchors or accepts replacement trust anchors at runtime.
func TrustedServerKeys() (protocol.ServerKeySet, error) {
	trusted, err := parseServerKeys(productionServerKeys)
	if err != nil {
		return nil, fmt.Errorf("license: embedded server keys: %w", err)
	}
	return trusted, nil
}

func parseServerKeys(value string) (protocol.ServerKeySet, error) {
	keys := protocol.ServerKeySet{}
	if strings.TrimSpace(value) == "" {
		return keys, nil
	}
	for _, entry := range strings.Split(value, ",") {
		id, encoded, ok := strings.Cut(strings.TrimSpace(entry), ":")
		if !ok || id == "" || encoded == "" {
			return nil, errors.New("license: trusted server keys must use id:base64url pairs")
		}
		if _, exists := keys[id]; exists {
			return nil, fmt.Errorf("license: duplicate trusted server key id %q", id)
		}
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("license: trusted server key %q is not a base64url ed25519 public key", id)
		}
		keys[id] = ed25519.PublicKey(key)
	}
	return keys, nil
}
