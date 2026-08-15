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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

// A swapped argument list still contains the collection and the property,
// so only the whole rendered path shows which segment each landed in.
func TestRenderedRoutes(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "indexes", got: IndexesRoute("Movies"), want: "GET /v1/schema/Movies/indexes"},
		{
			name: "cancel a searchable migration",
			got:  CancelRoute("Movies", "body", models.IndexStatusTypeSearchable),
			want: "POST /v1/schema/Movies/properties/body/index/searchable/cancel",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func TestClusterMigrationRemedy(t *testing.T) {
	remedy := ClusterMigrationRemedy()
	assert.Contains(t, remedy, "GET /v1/tasks")
	assert.NotContains(t, remedy, "<")
	assert.NotContains(t, remedy, "{")
}

func TestMigrationRemedy(t *testing.T) {
	remedy := MigrationRemedy("Movies")
	assert.Contains(t, remedy, "GET /v1/schema/Movies/indexes")
	assert.Contains(t, remedy, `status="pending"`,
		"a STARTED task with no progress reads as pending on the GET while the gate already refuses")
	assert.Contains(t, remedy, `status="indexing"`)
	assert.Contains(t, remedy, "POST /v1/schema/Movies/properties/<property>/index/<indexType>/cancel")
	assert.Contains(t, remedy, "filterable, searchable, rangeFilters",
		"the index-type segment must name the values the API accepts")
	assert.Contains(t, remedy, "accepted only while the task is STARTED")
	assert.Contains(t, remedy, "409")

	// Cancel is not accepted at every stage: the API's own predicate is
	// status == STARTED. A refusal that said otherwise would send an
	// operator at a call that refuses them.
	assert.NotContains(t, remedy, "every stage")
	assert.NotContains(t, remedy, "at any stage")
	assert.NotContains(t, remedy, "{className}",
		"a refusal must not hand an operator the swagger template")
	assert.NotContains(t, remedy, "<class>",
		"the collection is known, so it must be rendered")
}
