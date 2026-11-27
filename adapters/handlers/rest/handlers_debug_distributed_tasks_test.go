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

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
)

var registerShardNoopDebugHandlerOnce sync.Once

func TestShardNoopAddTaskPathElements(t *testing.T) {
	registerShardNoopDebugHandlerOnce.Do(func() { setupShardNoopDebugHandler(&state.State{}, nil) })

	for _, tc := range []struct {
		name        string
		id          string
		collection  string
		units       []string
		unitGroups  map[string]string
		unitToShard map[string]string
		wantField   string
	}{
		{name: "synthetic units", id: "unit-success-test", units: []string{"su-1", "su-2"}},
		{name: "grouped units", id: "group-many", units: []string{"grp-000-u0"}, unitGroups: map[string]string{"grp-000-u0": "grp-000", "u2": ""}},
		{name: "collection units", id: "suite-rf2-shards3", collection: "DTMSuiteRF2S3", units: []string{"Ab12cd__node1"}, unitToShard: map[string]string{"Ab12cd__node1": "Ab12cd"}},
		{name: "id with parent segments", id: "../../../../tmp/x", units: []string{"u1"}, wantField: "id"},
		{name: "id with separator", id: "a/b", wantField: "id"},
		{name: "id with backslash", id: `a\b`, wantField: "id"},
		{name: "id is dot-dot", id: "..", wantField: "id"},
		{name: "id is dot", id: ".", wantField: "id"},
		{name: "unit with parent segments", id: "t1", units: []string{"u1", "../../x"}, wantField: "units"},
		{name: "group with parent segments", id: "t1", units: []string{"u1"}, unitGroups: map[string]string{"u1": "../x"}, wantField: "unitGroups"},
		{name: "collection with parent segments", id: "t1", collection: "../../tmp/x", units: []string{"u1"}, wantField: "collection"},
		{name: "collection is dot-dot", id: "t1", collection: "..", units: []string{"u1"}, wantField: "collection"},
		{name: "shard with parent segments", id: "t1", collection: "C", units: []string{"u1"}, unitToShard: map[string]string{"u1": "../../x"}, wantField: "unitToShard"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantField, invalidShardNoopPathField(tc.id, tc.collection, tc.units, tc.unitGroups, tc.unitToShard))
			if tc.wantField == "" {
				return
			}
			body, err := json.Marshal(map[string]any{
				"id": tc.id, "collection": tc.collection, "units": tc.units,
				"unitGroups": tc.unitGroups, "unitToShard": tc.unitToShard,
			})
			require.NoError(t, err)
			rec := httptest.NewRecorder()
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("request was submitted instead of rejected: %v", r)
					}
				}()
				http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost,
					"/debug/distributed-tasks/add", bytes.NewReader(body)))
			}()
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), "invalid "+tc.wantField+" field")
		})
	}
}
