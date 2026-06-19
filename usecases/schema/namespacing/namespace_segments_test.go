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
	"testing"

	"github.com/stretchr/testify/assert"
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
