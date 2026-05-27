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

package reindex

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Pins the extractor wired into [Raft.RegisterDistributedTaskCollectionExtractor]
// for the reindex namespace (weaviate/0-weaviate-issues#231). A regression
// here would silently disable the DELETE_CLASS cascade for reindex tasks.
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
