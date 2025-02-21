//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package keys

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/alexedwards/argon2id"
	"github.com/stretchr/testify/require"
)

func TestKeyGeneration(t *testing.T) {
	fullApiKey, hash, userIdentifier, err := CreateApiKeyAndHash()
	require.NoError(t, err)

	randomKey, userIdentifierDecoded, err := DecodeApiKey(fullApiKey)
	require.NoError(t, err)
	require.Equal(t, userIdentifier, userIdentifierDecoded)

	match, err := argon2id.ComparePasswordAndHash(randomKey, hash)
	require.NoError(t, err)
	require.True(t, match)
}

func TestInvalidKeys(t *testing.T) {
	randomKeyDummy := strings.Repeat("A", RandomBytesBase64Length)
	randomIdentifierDummy := strings.Repeat("A", UserIdentifierBytesBase64Length)

	combiner := func(parts ...string) string {
		return strings.Join(parts, "_")
	}

	tests := []struct {
		name  string
		key   string
		error bool
	}{
		{name: "valid", key: combiner(randomKeyDummy, randomIdentifierDummy, DynUserIdentifier), error: false},
		{name: "invalid base64", key: "i am a string that is not base64", error: true},
		{name: "invalid version", key: combiner(randomKeyDummy, randomIdentifierDummy, "v123"), error: true},
		{name: "missing part", key: combiner(randomKeyDummy, randomIdentifierDummy), error: true},
		{name: "invalid randomKey", key: combiner(randomKeyDummy[:5], randomIdentifierDummy, DynUserIdentifier), error: true},
		{name: "invalid identifier", key: combiner(randomKeyDummy, randomIdentifierDummy[:5], DynUserIdentifier), error: true},
		{name: "all wrong", key: combiner(randomKeyDummy[:5], randomIdentifierDummy[:5], "v123"), error: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedKey := base64.StdEncoding.EncodeToString([]byte(tt.key))
			_, _, err := DecodeApiKey(encodedKey)
			if tt.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
