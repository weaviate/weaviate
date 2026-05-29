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

package apikey

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFilterDBUserData_AllMapsHonoured pins the multi-map invariant: every
// dbUserdata map must follow the filter, else restore yields a user with
// missing credentials, a broken identifier mapping, or a lifted revocation.
func TestFilterDBUserData_AllMapsHonoured(t *testing.T) {
	t.Parallel()

	// alice: every map. bob: required maps only. carol: full presence but
	// in a foreign namespace — must not survive a ns1 filter.
	src := dbUserdata{
		Users: map[string]*User{
			"ns1:alice": {Id: "alice", Namespace: "ns1", InternalIdentifier: "ident-alice"},
			"ns1:bob":   {Id: "bob", Namespace: "ns1", InternalIdentifier: "ident-bob"},
			"ns2:carol": {Id: "carol", Namespace: "ns2", InternalIdentifier: "ident-carol"},
		},
		SecureKeyStorageById: map[string]string{
			"ns1:alice": "hash-alice",
			"ns1:bob":   "hash-bob",
			"ns2:carol": "hash-carol",
		},
		IdToIdentifier: map[string]string{
			"ns1:alice": "ident-alice",
			"ns1:bob":   "ident-bob",
			"ns2:carol": "ident-carol",
		},
		IdentifierToId: map[string]string{
			"ident-alice": "ns1:alice",
			"ident-bob":   "ns1:bob",
			"ident-carol": "ns2:carol",
		},
		UserKeyRevoked: map[string]struct{}{
			"ns1:alice": {},
			"ns2:carol": {},
		},
		ImportedApiKeysWeakHash: map[string][sha256.Size]byte{
			"ns1:alice": sha256.Sum256([]byte("alice-imported")),
			"ns2:carol": sha256.Sum256([]byte("carol-imported")),
		},
	}

	tests := []struct {
		name string
		keep []string
		// per-map expectations localise the failing map on regression
		wantUsers                   []string
		wantSecureKeyStorageById    []string
		wantIdToIdentifier          []string
		wantIdentifierToId          []string // values to be present (keyed by identifier)
		wantUserKeyRevoked          []string
		wantImportedApiKeysWeakHash []string
		mustNotContain              []string // any id that should be absent from every map
	}{
		{
			name:                        "filter to a single namespaced user — all maps follow them",
			keep:                        []string{"ns1:alice"},
			wantUsers:                   []string{"ns1:alice"},
			wantSecureKeyStorageById:    []string{"ns1:alice"},
			wantIdToIdentifier:          []string{"ns1:alice"},
			wantIdentifierToId:          []string{"ident-alice"},
			wantUserKeyRevoked:          []string{"ns1:alice"},
			wantImportedApiKeysWeakHash: []string{"ns1:alice"},
			mustNotContain:              []string{"ns1:bob", "ns2:carol"},
		},
		{
			name:                     "filter to a user absent from optional maps — required maps populated, optionals empty",
			keep:                     []string{"ns1:bob"},
			wantUsers:                []string{"ns1:bob"},
			wantSecureKeyStorageById: []string{"ns1:bob"},
			wantIdToIdentifier:       []string{"ns1:bob"},
			wantIdentifierToId:       []string{"ident-bob"},
			// bob is in neither map; confirm no neighbour entries leak in.
			wantUserKeyRevoked:          nil,
			wantImportedApiKeysWeakHash: nil,
			mustNotContain:              []string{"ns1:alice", "ns2:carol"},
		},
		{
			name:                        "filter to a whole namespace — non-selected namespace fully dropped",
			keep:                        []string{"ns1:alice", "ns1:bob"},
			wantUsers:                   []string{"ns1:alice", "ns1:bob"},
			wantSecureKeyStorageById:    []string{"ns1:alice", "ns1:bob"},
			wantIdToIdentifier:          []string{"ns1:alice", "ns1:bob"},
			wantIdentifierToId:          []string{"ident-alice", "ident-bob"},
			wantUserKeyRevoked:          []string{"ns1:alice"},
			wantImportedApiKeysWeakHash: []string{"ns1:alice"},
			mustNotContain:              []string{"ns2:carol", "ident-carol"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := filterDBUserData(src, tt.keep)
			require.NoError(t, err)

			require.ElementsMatch(t, tt.wantUsers, mapKeys(out.Users))
			require.ElementsMatch(t, tt.wantSecureKeyStorageById, mapKeys(out.SecureKeyStorageById))
			require.ElementsMatch(t, tt.wantIdToIdentifier, mapKeys(out.IdToIdentifier))
			require.ElementsMatch(t, tt.wantIdentifierToId, mapKeys(out.IdentifierToId))
			require.ElementsMatch(t, tt.wantUserKeyRevoked, mapKeys(out.UserKeyRevoked))
			require.ElementsMatch(t, tt.wantImportedApiKeysWeakHash, mapKeys(out.ImportedApiKeysWeakHash))

			// No non-selected id may leak — including via IdentifierToId's value side.
			for _, id := range tt.mustNotContain {
				require.NotContains(t, out.Users, id, "Users")
				require.NotContains(t, out.SecureKeyStorageById, id, "SecureKeyStorageById")
				require.NotContains(t, out.IdToIdentifier, id, "IdToIdentifier")
				require.NotContains(t, out.IdentifierToId, id, "IdentifierToId (by identifier)")
				for _, v := range out.IdentifierToId {
					require.NotEqual(t, id, v, "IdentifierToId (by user id value)")
				}
				require.NotContains(t, out.UserKeyRevoked, id, "UserKeyRevoked")
				require.NotContains(t, out.ImportedApiKeysWeakHash, id, "ImportedApiKeysWeakHash")
			}

			// Pin identity (pointer for *User, bytes for hashes) — guards
			// against the filter accidentally reconstructing values.
			if _, ok := out.Users["ns1:alice"]; ok {
				require.Same(t, src.Users["ns1:alice"], out.Users["ns1:alice"])
				require.Equal(t, src.SecureKeyStorageById["ns1:alice"], out.SecureKeyStorageById["ns1:alice"])
				require.Equal(t, src.ImportedApiKeysWeakHash["ns1:alice"], out.ImportedApiKeysWeakHash["ns1:alice"])
			}
		})
	}
}

// TestFilterDBUserData_RejectsUnknownID pins the error-vs-skip choice:
// silent skip would let a graduation backup omit users — undetectable by
// integration tests.
func TestFilterDBUserData_RejectsUnknownID(t *testing.T) {
	t.Parallel()

	src := dbUserdata{
		Users: map[string]*User{
			"ns1:alice": {Id: "alice", Namespace: "ns1"},
		},
	}

	out, err := filterDBUserData(src, []string{"ns1:alice", "ns1:ghost"})
	require.Error(t, err)
	require.Contains(t, err.Error(), `"ns1:ghost"`)
	// On error, returned value must be empty — callers shouldn't inspect both.
	require.Empty(t, out.Users)
}

func mapKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestStripDBUserNamespace_AllMapsHonoured pins the multi-map *rewrite*
// invariant — distinct from the filter test because the rewrite touches
// the IdentifierToId value side, User.Id, and User.Namespace clearing.
func TestStripDBUserNamespace_AllMapsHonoured(t *testing.T) {
	t.Parallel()

	// alice: every map. bob: partial (no revocation, no imported hash).
	// global: bare id; must pass through unchanged.
	src := dbUserdata{
		Users: map[string]*User{
			"ns1:alice": {Id: "ns1:alice", Namespace: "ns1", InternalIdentifier: "ident-alice"},
			"ns1:bob":   {Id: "ns1:bob", Namespace: "ns1", InternalIdentifier: "ident-bob"},
			"global":    {Id: "global", Namespace: "", InternalIdentifier: "ident-global"},
		},
		SecureKeyStorageById: map[string]string{
			"ns1:alice": "hash-alice",
			"ns1:bob":   "hash-bob",
			"global":    "hash-global",
		},
		IdToIdentifier: map[string]string{
			"ns1:alice": "ident-alice",
			"ns1:bob":   "ident-bob",
			"global":    "ident-global",
		},
		IdentifierToId: map[string]string{
			"ident-alice":  "ns1:alice",
			"ident-bob":    "ns1:bob",
			"ident-global": "global",
		},
		UserKeyRevoked: map[string]struct{}{
			"ns1:alice": {},
		},
	}

	out, err := stripDBUserNamespace(src)
	require.NoError(t, err)

	require.ElementsMatch(t, []string{"alice", "bob", "global"}, mapKeys(out.Users))
	require.ElementsMatch(t, []string{"alice", "bob", "global"}, mapKeys(out.SecureKeyStorageById))
	require.ElementsMatch(t, []string{"alice", "bob", "global"}, mapKeys(out.IdToIdentifier))
	require.ElementsMatch(t, []string{"alice"}, mapKeys(out.UserKeyRevoked))

	// IdentifierToId: keys unchanged, values stripped.
	require.ElementsMatch(t, []string{"ident-alice", "ident-bob", "ident-global"}, mapKeys(out.IdentifierToId))
	require.Equal(t, "alice", out.IdentifierToId["ident-alice"])
	require.Equal(t, "bob", out.IdentifierToId["ident-bob"])
	require.Equal(t, "global", out.IdentifierToId["ident-global"])

	// Stripped entries: Id rewritten, Namespace cleared.
	require.Equal(t, "alice", out.Users["alice"].Id)
	require.Empty(t, out.Users["alice"].Namespace)
	require.Equal(t, "bob", out.Users["bob"].Id)
	require.Empty(t, out.Users["bob"].Namespace)
	require.Equal(t, "global", out.Users["global"].Id)
	require.Empty(t, out.Users["global"].Namespace)

	require.Equal(t, "hash-alice", out.SecureKeyStorageById["alice"])
	require.Equal(t, "ident-alice", out.IdToIdentifier["alice"])

	// Fresh User — caller mutations must not reach back into src.
	require.NotSame(t, src.Users["ns1:alice"], out.Users["alice"])
}

// TestStripDBUserNamespace_RejectsCollision: silent overwrite of a colliding
// id would corrupt credentials with no other test catching it.
func TestStripDBUserNamespace_RejectsCollision(t *testing.T) {
	t.Parallel()

	src := dbUserdata{
		Users: map[string]*User{
			"ns1:alice": {Id: "ns1:alice", Namespace: "ns1"},
			"alice":     {Id: "alice", Namespace: ""},
		},
	}

	out, err := stripDBUserNamespace(src)
	require.Error(t, err)
	require.Contains(t, err.Error(), `"alice"`)
	require.Empty(t, out.Users)
}
