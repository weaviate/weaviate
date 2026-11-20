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
	"testing"

	"github.com/stretchr/testify/require"

	protocol "github.com/weaviate/weaviate/entities/license"
)

func TestTrustedServerKeys(t *testing.T) {
	keys, err := TrustedServerKeys()
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Equal(t, "kSqW93_D-AqI", protocol.KeyIDFor(keys["kSqW93_D-AqI"]))
	require.Equal(t, "QtbmlPpmSqodZMBBP2nNyjNyGRNjz3Ejm_KXtNrMVFs",
		base64.RawURLEncoding.EncodeToString(keys["kSqW93_D-AqI"]))
}

func TestTrustedServerKeysRelinkedForTest(t *testing.T) {
	original := productionServerKeys
	t.Cleanup(func() { productionServerKeys = original })
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	productionServerKeys = "test:" + base64.RawURLEncoding.EncodeToString(pub)

	keys, err := TrustedServerKeys()
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, pub, keys["test"])
}

func TestParseServerKeysRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
	}{
		{name: "missing separator", value: "test"},
		{name: "empty id", value: ":QtbmlPpmSqodZMBBP2nNyjNyGRNjz3Ejm_KXtNrMVFs"},
		{name: "empty key", value: "test:"},
		{name: "invalid base64", value: "test:not+base64url"},
		{name: "wrong key length", value: "test:c2hvcnQ"},
		{name: "duplicate id", value: "test:QtbmlPpmSqodZMBBP2nNyjNyGRNjz3Ejm_KXtNrMVFs,test:QtbmlPpmSqodZMBBP2nNyjNyGRNjz3Ejm_KXtNrMVFs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseServerKeys(tc.value)
			require.Error(t, err)
		})
	}
}
