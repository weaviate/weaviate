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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// A merge keeps the token ids of the multi-vectors it leaves as they were,
// takes those it brings, and drops those of a multi-vector it replaced
// without bringing its ids.
func TestMergeTokenIDs(t *testing.T) {
	previous := &storobj.Object{
		Object: models.Object{TokenIds: models.TokenIds{"a": {1, 2}, "b": {3}}},
		MultiVectors: map[string][][]float32{
			"a": {{1, 0}, {0, 1}},
			"b": {{1, 1}},
		},
	}
	tests := []struct {
		name   string
		next   map[string][][]float32
		merged models.TokenIds
		want   models.TokenIds
	}{
		{
			name: "nothing changed",
			next: previous.MultiVectors,
			want: models.TokenIds{"a": {1, 2}, "b": {3}},
		},
		{
			name: "b replaced without ids",
			next: map[string][][]float32{"a": {{1, 0}, {0, 1}}, "b": {{2, 2}}},
			want: models.TokenIds{"a": {1, 2}},
		},
		{
			name:   "b replaced with ids",
			next:   map[string][][]float32{"a": {{1, 0}, {0, 1}}, "b": {{2, 2}, {3, 3}}},
			merged: models.TokenIds{"b": {8, 9}},
			want:   models.TokenIds{"a": {1, 2}, "b": {8, 9}},
		},
		{
			name: "every multi-vector replaced without ids",
			next: map[string][][]float32{"a": {{5, 5}}},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, mergeTokenIDs(previous, tt.next, tt.merged))
		})
	}
}
