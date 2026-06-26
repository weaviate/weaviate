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

package namespacing

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindNamespaceSegments(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		wantStart int
		wantEnd   int
		wantAlias bool
		wantSeg   string // path[start:end] for readability; "" when not namespaceable
	}{
		{
			name:      "schema collection",
			path:      "schema/collections/Movies/shards/#",
			wantStart: len(SchemaCollectionsPrefix),
			wantEnd:   len("schema/collections/Movies"),
			wantSeg:   "Movies",
		},
		{
			name:      "data collection",
			path:      "data/collections/customer1:Movies/shards/.*/objects/.*",
			wantStart: len(DataCollectionsPrefix),
			wantEnd:   len("data/collections/customer1:Movies"),
			wantSeg:   "customer1:Movies",
		},
		{
			name:      "aliases collection + alias (two segments)",
			path:      "aliases/collections/Movies/aliases/Films",
			wantStart: len(AliasesCollectionsPrefix),
			wantEnd:   len("aliases/collections/Movies"),
			wantAlias: true,
			wantSeg:   "Movies",
		},
		{
			name:      "users terminal",
			path:      "users/customer1:bob",
			wantStart: len(UsersPrefix),
			wantEnd:   len("users/customer1:bob"),
			wantSeg:   "customer1:bob",
		},
		{
			name:      "roles terminal",
			path:      "roles/customer1:editor",
			wantStart: len(RolesPrefix),
			wantEnd:   len("roles/customer1:editor"),
			wantSeg:   "customer1:editor",
		},
		{
			name:    "schema without /shards/ → not namespaceable",
			path:    "schema/collections/Movies",
			wantEnd: 0,
		},
		{
			name:    "groups/ deliberately not namespaceable (colon is a literal group-id char)",
			path:    "groups/oidc/customer1:team",
			wantEnd: 0,
		},
		{
			name:    "unknown shape → not namespaceable",
			path:    "cluster/whatever",
			wantEnd: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, hasAlias := FindNamespaceSegments(tt.path)
			assert.Equal(t, tt.wantEnd, end)
			assert.Equal(t, tt.wantAlias, hasAlias)
			if tt.wantEnd == 0 {
				return
			}
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantSeg, tt.path[start:end])
		})
	}
}

func TestSegmentHasSeparator(t *testing.T) {
	path := "roles/customer1:editor"
	start, end, _ := FindNamespaceSegments(path)
	assert.True(t, SegmentHasSeparator(path, start, end), "qualified role segment has separator")

	path = "roles/editor"
	start, end, _ = FindNamespaceSegments(path)
	assert.False(t, SegmentHasSeparator(path, start, end), "bare role segment has no separator")
}

// TestRewriteNamespaceSegments pins the shared structural walk — first segment,
// the alias second segment at end+len("/aliases/"), the non-namespaceable
// passthrough, and abort-on-error — so the three call sites that build on it
// (qualify, project, enforce-rewrite) cannot drift.
func TestRewriteNamespaceSegments(t *testing.T) {
	// upper rewrites each visited segment so both visits are observable.
	upper := func(seg string) (string, error) { return strings.ToUpper(seg), nil }

	t.Run("single collection segment", func(t *testing.T) {
		var seen []string
		out, err := RewriteNamespaceSegments("data/collections/Movies/shards/.*/objects/.*", func(seg string) (string, error) {
			seen = append(seen, seg)
			return "X:" + seg, nil
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"Movies"}, seen)
		assert.Equal(t, "data/collections/X:Movies/shards/.*/objects/.*", out)
	})

	t.Run("both alias segments visited, surrounding text preserved", func(t *testing.T) {
		var seen []string
		out, err := RewriteNamespaceSegments("aliases/collections/Movies/aliases/Films", func(seg string) (string, error) {
			seen = append(seen, seg)
			return strings.ToUpper(seg), nil
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"Movies", "Films"}, seen)
		assert.Equal(t, "aliases/collections/MOVIES/aliases/FILMS", out)
	})

	t.Run("non-namespaceable path passes through untouched", func(t *testing.T) {
		out, err := RewriteNamespaceSegments("cluster/*", func(string) (string, error) {
			t.Fatal("fn must not be called for a non-namespaceable path")
			return "", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "cluster/*", out)
	})

	t.Run("fn error on first segment aborts", func(t *testing.T) {
		sentinel := errors.New("boom")
		_, err := RewriteNamespaceSegments("schema/collections/Movies/shards/#", func(string) (string, error) {
			return "", sentinel
		})
		assert.ErrorIs(t, err, sentinel)
	})

	t.Run("fn error on alias second segment aborts", func(t *testing.T) {
		sentinel := errors.New("boom")
		_, err := RewriteNamespaceSegments("aliases/collections/Movies/aliases/Films", func(seg string) (string, error) {
			if seg == "Films" {
				return "", sentinel
			}
			return upper(seg)
		})
		assert.ErrorIs(t, err, sentinel)
	})
}

// TestGlobalCallerWidens pins the widening verdict for every registered shape so
// adding a prefix to FindNamespaceSegments without revisiting this rule trips a
// test rather than silently changing matcher behavior.
func TestGlobalCallerWidens(t *testing.T) {
	tests := []struct {
		name   string
		reqObj string
		want   bool
	}{
		{"bare schema collection does not widen", "schema/collections/Movies/shards/#", false},
		{"qualified schema collection widens", "schema/collections/customer1:Movies/shards/#", true},
		{"bare role does not widen", "roles/editor", false},
		{"bare user does not widen", "users/alice", false},
		// users/ is the carve-out: a ':' in a user id is opaque, never a namespace.
		{"colon-bearing user does not widen", "users/customer1:alice", false},
		// groups/ is unregistered, so the ':'-heuristic widens here; the matcher's
		// downstream shape check still matches groups literally. Registering groups/
		// without carving it out would make this widening take effect.
		{"colon-bearing group widens at this layer", "groups/oidc/team:eng", true},
		{"non-namespaceable colon-free path does not widen", "backups/mybackup", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, GlobalCallerWidens(tt.reqObj))
		})
	}
}
