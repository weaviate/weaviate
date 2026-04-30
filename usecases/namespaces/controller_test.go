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
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

func newTestController(t *testing.T) *Controller {
	t.Helper()
	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	return NewController(logger)
}

func TestValidateName(t *testing.T) {
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
		// Explicit NamespaceSeparator coverage: the reserved separator must
		// never appear in a namespace name regardless of position, so that
		// "<ns>:<entity>" qualified identifiers stay unambiguously parseable.
		{name: "namespace separator middle", input: "foo" + entschema.NamespaceSeparator + "bar", wantErr: true},
		{name: "namespace separator leading", input: entschema.NamespaceSeparator + "foo", wantErr: true},
		{name: "namespace separator trailing", input: "foo" + entschema.NamespaceSeparator, wantErr: true},
		{name: "only namespace separator", input: entschema.NamespaceSeparator, wantErr: true},
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

func TestController_Create(t *testing.T) {
	tests := []struct {
		name    string
		seed    []string
		input   cmd.Namespace
		wantErr error
	}{
		{
			name:  "happy path",
			input: cmd.Namespace{Name: "customer1"},
		},
		{
			name:    "duplicate is rejected with ErrAlreadyExists",
			seed:    []string{"customer1"},
			input:   cmd.Namespace{Name: "customer1"},
			wantErr: ErrAlreadyExists,
		},
		{
			name:    "invalid name is rejected",
			input:   cmd.Namespace{Name: "BadName"},
			wantErr: ErrBadRequest,
		},
		{
			name:    "reserved name is rejected",
			input:   cmd.Namespace{Name: "admin"},
			wantErr: ErrBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name}))
			}
			err := c.Create(tc.input)
			if tc.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, len(tc.seed)+1, c.Count())
		})
	}
}

func TestController_Delete(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1"}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2"}))

	t.Run("delete existing", func(t *testing.T) {
		require.NoError(t, c.Delete("customer1"))
		assert.Equal(t, 1, c.Count())
	})

	t.Run("delete missing returns ErrNotFound", func(t *testing.T) {
		err := c.Delete("customer1") // already deleted above
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		err = c.Delete("never-existed")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		// State was not altered by the failed deletes.
		assert.Equal(t, 1, c.Count())
	})
}

func TestController_Get(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1"}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2"}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer3"}))

	tests := []struct {
		name  string
		query []string
		want  []string
	}{
		{
			name: "empty names returns all",
			want: []string{"customer1", "customer2", "customer3"},
		},
		{
			name:  "specific names returns subset",
			query: []string{"customer1", "customer3"},
			want:  []string{"customer1", "customer3"},
		},
		{
			name:  "missing names are omitted",
			query: []string{"customer1", "never-existed"},
			want:  []string{"customer1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.ElementsMatch(t, tc.want, namesOf(c.Get(tc.query...)))
		})
	}
}

func TestController_Exists(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1"}))

	assert.True(t, c.Exists("customer1"))
	assert.False(t, c.Exists("never-existed"))

	require.NoError(t, c.Delete("customer1"))
	assert.False(t, c.Exists("customer1"))
}

func TestController_List(t *testing.T) {
	c := newTestController(t)
	assert.Empty(t, c.List())

	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1"}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2"}))

	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(c.List()))
}

func TestController_SnapshotRestoreRoundtrip(t *testing.T) {
	c := newTestController(t)
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer1"}))
	require.NoError(t, c.Create(cmd.Namespace{Name: "customer2"}))

	snap, err := c.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, snap)

	restored := newTestController(t)
	require.NoError(t, restored.Restore(snap))
	assert.Equal(t, 2, restored.Count())
	assert.ElementsMatch(t,
		[]string{"customer1", "customer2"},
		namesOf(restored.List()))
}

func TestController_Restore(t *testing.T) {
	tests := []struct {
		name      string
		seed      []string
		snap      []byte
		wantErr   bool
		wantNames []string
	}{
		{
			// Cold start / fresh bootstrap: no snapshot bytes yet.
			name: "nil snapshot resets state",
			seed: []string{"stale"},
			snap: nil,
		},
		{
			name: "empty snapshot resets state",
			seed: []string{"stale"},
			snap: []byte{},
		},
		{
			// Snapshots that carry unknown fields (e.g. a future `state`
			// field on Namespace) must still restore cleanly.
			name:      "unknown fields are tolerated",
			snap:      []byte(`{"customer1":{"Name":"customer1","Restrictions":{},"FutureField":"ignored"}}`),
			wantNames: []string{"customer1"},
		},
		{
			name:    "malformed JSON returns an error",
			snap:    []byte("not-json"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestController(t)
			for _, name := range tc.seed {
				require.NoError(t, c.Create(cmd.Namespace{Name: name}))
			}
			err := c.Restore(tc.snap)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.wantNames, namesOf(c.List()))
		})
	}
}

func namesOf(nss []cmd.Namespace) []string {
	out := make([]string, len(nss))
	for i, ns := range nss {
		out[i] = ns.Name
	}
	return out
}
