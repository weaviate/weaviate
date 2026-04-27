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

package namespaces

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewManager(logger)
}

func addCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.AddNamespaceRequest{Namespace: cmd.Namespace{Name: name}})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func deleteCmd(t *testing.T, name string) *cmd.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(cmd.DeleteNamespaceRequest{Name: name})
	require.NoError(t, err)
	return &cmd.ApplyRequest{SubCommand: payload}
}

func TestValidateNamespaceName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		// valid
		{name: "short valid", input: "foo", wantErr: false},
		{name: "letters and digits", input: "customer1", wantErr: false},
		{name: "max length", input: strings.Repeat("a", NameMaxLength), wantErr: false},
		// length
		{name: "too short", input: "ab", wantErr: true},
		{name: "empty", input: "", wantErr: true},
		{name: "too long", input: strings.Repeat("a", NameMaxLength+1), wantErr: true},
		// format
		{name: "uppercase leading", input: "Foo", wantErr: true},
		{name: "uppercase middle", input: "fooBar", wantErr: true},
		{name: "digit leading", input: "1foo", wantErr: true},
		{name: "underscore", input: "foo_bar", wantErr: true},
		{name: "hyphen", input: "foo-bar", wantErr: true},
		{name: "contains colon", input: "foo:bar", wantErr: true},
		{name: "whitespace", input: "foo bar", wantErr: true},
		// reserved
		{name: "reserved admin", input: "admin", wantErr: true},
		{name: "reserved system", input: "system", wantErr: true},
		{name: "reserved default", input: "default", wantErr: true},
		{name: "reserved internal", input: "internal", wantErr: true},
		{name: "reserved weaviate", input: "weaviate", wantErr: true},
		{name: "reserved global", input: "global", wantErr: true},
		{name: "reserved public", input: "public", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateName(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestManager_Add(t *testing.T) {
	m := newTestManager(t)

	require.NoError(t, m.Add(addCmd(t, "customer1")))
	assert.Equal(t, 1, m.Count())

	t.Run("duplicate is rejected with ErrAlreadyExists", func(t *testing.T) {
		err := m.Add(addCmd(t, "customer1"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAlreadyExists)
	})

	t.Run("invalid name is rejected", func(t *testing.T) {
		err := m.Add(addCmd(t, "BadName"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBadRequest)
	})

	t.Run("reserved name is rejected", func(t *testing.T) {
		err := m.Add(addCmd(t, "admin"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBadRequest)
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		err := m.Add(&cmd.ApplyRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBadRequest)
	})
}

func TestManager_Delete(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	require.NoError(t, m.Add(addCmd(t, "customer2")))

	t.Run("delete existing", func(t *testing.T) {
		require.NoError(t, m.Delete(deleteCmd(t, "customer1")))
		assert.Equal(t, 1, m.Count())
	})

	t.Run("delete missing returns ErrNotFound", func(t *testing.T) {
		err := m.Delete(deleteCmd(t, "customer1")) // already deleted above
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		err = m.Delete(deleteCmd(t, "never-existed"))
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		// State was not altered by the failed deletes.
		assert.Equal(t, 1, m.Count())
	})

	t.Run("malformed payload is rejected", func(t *testing.T) {
		err := m.Delete(&cmd.ApplyRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBadRequest)
	})
}

func TestManager_Get(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	require.NoError(t, m.Add(addCmd(t, "customer2")))
	require.NoError(t, m.Add(addCmd(t, "customer3")))

	get := func(t *testing.T, names ...string) []cmd.Namespace {
		t.Helper()
		payload, err := json.Marshal(cmd.QueryGetNamespacesRequest{Names: names})
		require.NoError(t, err)
		raw, err := m.Get(&cmd.QueryRequest{SubCommand: payload})
		require.NoError(t, err)
		var resp cmd.QueryGetNamespacesResponse
		require.NoError(t, json.Unmarshal(raw, &resp))
		return resp.Namespaces
	}

	t.Run("empty names returns all", func(t *testing.T) {
		got := get(t)
		gotNames := namesOf(got)
		assert.ElementsMatch(t, []string{"customer1", "customer2", "customer3"}, gotNames)
	})

	t.Run("specific names returns subset", func(t *testing.T) {
		got := get(t, "customer1", "customer3")
		assert.ElementsMatch(t, []string{"customer1", "customer3"}, namesOf(got))
	})

	t.Run("missing names are omitted", func(t *testing.T) {
		got := get(t, "customer1", "never-existed")
		assert.ElementsMatch(t, []string{"customer1"}, namesOf(got))
	})
}

func TestManager_SnapshotRestoreRoundtrip(t *testing.T) {
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "customer1")))
	require.NoError(t, m.Add(addCmd(t, "customer2")))

	snap, err := m.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	restored := newTestManager(t)
	require.NoError(t, restored.Restore(snap))
	assert.Equal(t, 2, restored.Count())
	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(restored.List()),
	)
}

func TestManager_RestoreEmpty(t *testing.T) {
	// Cold start / fresh bootstrap: no snapshot bytes yet.
	m := newTestManager(t)
	require.NoError(t, m.Add(addCmd(t, "stale")))

	require.NoError(t, m.Restore(nil))
	assert.Equal(t, 0, m.Count())
}

func TestManager_RestoreUnknownFields(t *testing.T) {
	// Forward-compatibility contract: a snapshot that carries fields we
	// don't yet know about (e.g. a future `state` field on Namespace) must
	// still restore cleanly so older binaries can read newer snapshots —
	// or, more realistically here, so tests can encode today what tomorrow
	// will add.
	snap := []byte(`{"customer1":{"Name":"customer1","Restrictions":{},"FutureField":"ignored"}}`)
	m := newTestManager(t)
	require.NoError(t, m.Restore(snap))
	assert.Equal(t, 1, m.Count())
	names := namesOf(m.List())
	assert.Equal(t, []string{"customer1"}, names)
}

func namesOf(nss []cmd.Namespace) []string {
	out := make([]string, len(nss))
	for i, ns := range nss {
		out[i] = ns.Name
	}
	return out
}
