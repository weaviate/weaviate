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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Pins the DELETE_CLASS cascade extractor for the reindex namespace
// (weaviate/0-weaviate-issues#231).
func TestExtractReindexTaskCollection(t *testing.T) {
	t.Run("well-formed payload returns class name", func(t *testing.T) {
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeChangeTokenization,
			Collection:    "ProductCatalog",
		})
		assert.NoError(t, err)

		got, ok := ExtractReindexTaskCollection(payload)
		assert.True(t, ok)
		assert.Equal(t, "ProductCatalog", got)
	})

	t.Run("payload missing collection is rejected", func(t *testing.T) {
		// Sloppy ("", true) here would let DeleteTasksForCollection("")
		// nuke every reindex task — defence in depth even though the
		// manager already guards empty input.
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeEnableRangeable,
		})
		assert.NoError(t, err)

		got, ok := ExtractReindexTaskCollection(payload)
		assert.False(t, ok)
		assert.Equal(t, "", got)
	})

	t.Run("unparseable payload is rejected", func(t *testing.T) {
		got, ok := ExtractReindexTaskCollection([]byte("not json"))
		assert.False(t, ok)
		assert.Equal(t, "", got)
	})

	t.Run("payload with extra fields still parses", func(t *testing.T) {
		// ReindexTaskPayload may gain fields over time; the extractor must
		// stay forwards-compatible with unknown JSON keys.
		got, ok := ExtractReindexTaskCollection([]byte(
			`{"collection":"Foo","migrationType":"change-tokenization","futureField":42}`))
		assert.True(t, ok)
		assert.Equal(t, "Foo", got)
	})
}

func TestExtractReindexTaskReplicas(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
		want    map[string][]string
		wantOK  bool
	}{
		{
			name: "units group by shard",
			payload: []byte(`{"unitToShard":{"s1__n1":"s1","s1__n2":"s1","s2__n1":"s2"},` +
				`"unitToNode":{"s1__n1":"n1","s1__n2":"n2","s2__n1":"n1"}}`),
			want:   map[string][]string{"s1": {"n1", "n2"}, "s2": {"n1"}},
			wantOK: true,
		},
		{name: "unparseable payload is rejected", payload: []byte("not json")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractReindexTaskReplicas(tc.payload)
			require.Equal(t, tc.wantOK, ok)
			require.Len(t, got, len(tc.want))
			for shard, nodes := range tc.want {
				require.ElementsMatch(t, nodes, got[shard])
			}
		})
	}
}
