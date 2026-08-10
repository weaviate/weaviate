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
)

// Pins the extractor wired into [Raft.RegisterDistributedTaskCollectionExtractor]
// for the reindex namespace (weaviate/0-weaviate-issues#231). A regression here
// would silently disable the DELETE_CLASS cascade for reindex tasks.
//
// The unreadable-payload rows are the ones that matter most: the task an
// operator most needs to delete is the one no node can decode, because that is
// the one holding backups hostage. An extractor that gives up on it leaves the
// completed-task TTL as the only remedy.
func TestExtractReindexTaskCollection(t *testing.T) {
	wellFormed, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Collection:    "ProductCatalog",
	})
	assert.NoError(t, err)

	noCollection, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeEnableRangeable,
	})
	assert.NoError(t, err)

	tests := []struct {
		name    string
		payload []byte
		want    string
		wantOK  bool
		why     string
	}{
		{
			name:    "well-formed payload returns class name",
			payload: wellFormed,
			want:    "ProductCatalog",
			wantOK:  true,
		},
		{
			name:    "payload missing collection is rejected",
			payload: noCollection,
			// Sloppy ("", true) here would let DeleteTasksForCollection("")
			// nuke every reindex task — defence in depth even though the
			// manager already guards empty input.
			why: "an empty name must never scope a cascade delete",
		},
		{
			name:    "fully decodable, with units",
			payload: []byte(`{"collection":"Movies","unitToShard":{"u1":"s1"}}`),
			want:    "Movies",
			wantOK:  true,
		},
		{
			name:    "a field retyped by a newer node",
			payload: poisonPayload("Movies"),
			want:    "Movies",
			wantOK:  true,
			why:     "the rolling-upgrade case this fallback exists for; the collection is still right there",
		},
		{
			name:    "payload with extra fields still parses",
			payload: []byte(`{"collection":"Foo","migrationType":"change-tokenization","futureField":42}`),
			want:    "Foo",
			wantOK:  true,
			why:     "ReindexTaskPayload may gain fields, so unknown JSON keys must not break the extractor",
		},
		{
			name:    "not JSON at all",
			payload: []byte("{not json"),
		},
		{
			name:    "unparseable, not even brace-prefixed",
			payload: []byte("not json"),
		},
		{
			name:    "collection itself retyped",
			payload: []byte(`{"collection":42}`),
			why:     "the one field the fallback reads is the one a newer node broke; there is nothing left to scope by",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ExtractReindexTaskCollection(tc.payload)
			assert.Equal(t, tc.want, got, tc.why)
			assert.Equal(t, tc.wantOK, ok,
				"a task the extractor cannot scope is skipped by DeleteTasksForCollection")
		})
	}
}

// poisonPayload defeats the full [ReindexTaskPayload] decoder while leaving the
// collection readable: unitToShard is a map today, and a newer node shipping it
// as anything else is the concrete rolling-upgrade case these fallbacks exist
// for. Shared with the backup overlap tests.
func poisonPayload(collection string) []byte {
	return []byte(`{"collection":"` + collection + `","unitToShard":"a-newer-node-changed-this-shape"}`)
}
