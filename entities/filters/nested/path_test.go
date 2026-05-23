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

package nested

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSegmentIndex(t *testing.T) {
	tests := []struct {
		name      string
		seg       string
		wantClean string
		wantIdx   int
		wantHas   bool
	}{
		// valid cases
		{"index at end", "tags[2]", "tags", 2, true},
		{"zero index", "tags[0]", "tags", 0, true},
		{"multi-digit index", "cars[10]", "cars", 10, true},
		{"index only — empty name", "[3]", "", 3, true},
		// no index
		{"plain segment", "tags", "tags", 0, false},
		{"empty string", "", "", 0, false},
		// bracket not at the end — treated as no index
		{"bracket in the middle", "ta[2]gs", "ta[2]gs", 0, false},
		{"trailing chars after bracket", "tags[2]x", "tags[2]x", 0, false},
		// malformed
		{"empty index", "tags[]", "tags[]", 0, false},
		{"negative index", "tags[-1]", "tags[-1]", 0, false},
		{"non-numeric index", "tags[abc]", "tags[abc]", 0, false},
		{"unclosed bracket", "tags[", "tags[", 0, false},
		{"no opening bracket", "tags]", "tags]", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clean, idx, has := parseSegmentIndex(tt.seg)
			assert.Equal(t, tt.wantClean, clean)
			assert.Equal(t, tt.wantIdx, idx)
			assert.Equal(t, tt.wantHas, has)
		})
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want []PathSegment
	}{
		{
			name: "single segment no index",
			path: "addresses",
			want: []PathSegment{{Name: "addresses"}},
		},
		{
			name: "single segment with index",
			path: "addresses[1]",
			want: []PathSegment{{Name: "addresses", HasIndex: true, Index: 1}},
		},
		{
			name: "two segments no index",
			path: "addresses.city",
			want: []PathSegment{{Name: "addresses"}, {Name: "city"}},
		},
		{
			name: "root index with sub-property",
			path: "addresses[1].city",
			want: []PathSegment{{Name: "addresses", HasIndex: true, Index: 1}, {Name: "city"}},
		},
		{
			name: "sub-property index",
			path: "cars.tags[2]",
			want: []PathSegment{{Name: "cars"}, {Name: "tags", HasIndex: true, Index: 2}},
		},
		{
			name: "multi-level indexes",
			path: "cars[1].tags[2]",
			want: []PathSegment{
				{Name: "cars", HasIndex: true, Index: 1},
				{Name: "tags", HasIndex: true, Index: 2},
			},
		},
		{
			name: "three segments no index",
			path: "cars.tires.width",
			want: []PathSegment{{Name: "cars"}, {Name: "tires"}, {Name: "width"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SplitPath(tt.path))
		})
	}
}

func TestParseIndexedPath(t *testing.T) {
	tests := []struct {
		name             string
		path             string
		wantRelPath      string
		wantArrayIndices []ArrayIndex
	}{
		{
			name:        "no index — plain nested path unchanged",
			path:        "addresses.city",
			wantRelPath: "city",
		},
		{
			name:             "root-level index — addresses[1].city",
			path:             "addresses[1].city",
			wantRelPath:      "city",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}},
		},
		{
			name:             "leaf scalar array index — cars.tires.radiuses[2]",
			path:             "cars.tires.radiuses[2]",
			wantRelPath:      "tires.radiuses",
			wantArrayIndices: []ArrayIndex{{RelPath: "tires.radiuses", Index: 2}},
		},
		{
			name:             "multi-level index — cars[1].tags[2]",
			path:             "cars[1].tags[2]",
			wantRelPath:      "tags",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}, {RelPath: "tags", Index: 2}},
		},
		{
			name:             "root only with index — addresses[1]",
			path:             "addresses[1]",
			wantRelPath:      "",
			wantArrayIndices: []ArrayIndex{{RelPath: "", Index: 1}},
		},
		{
			name:             "zero index — tags[0]",
			path:             "cars.tags[0]",
			wantRelPath:      "tags",
			wantArrayIndices: []ArrayIndex{{RelPath: "tags", Index: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relPath, _, arrayIndices := ParseIndexedPath(tt.path)
			assert.Equal(t, tt.wantRelPath, relPath)
			assert.Equal(t, tt.wantArrayIndices, arrayIndices)
		})
	}
}

func TestRootPropName(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"addresses.city", "addresses"},
		{"addresses[1].city", "addresses"},
		{"addresses[1]", "addresses"},
		{"cars[0].tires.width", "cars"},
		{"name", "name"},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assert.Equal(t, tt.want, RootPropName(tt.path))
		})
	}
}
