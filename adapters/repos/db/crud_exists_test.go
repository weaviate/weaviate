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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// Repeats because Go map order varies per walk.
func TestExistsWithoutClass(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	otherID := strfmt.UUID("0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e")

	type collection struct {
		class       string
		multiTenant bool
		holds       strfmt.UUID
	}
	cases := []struct {
		name        string
		collections []collection
		want        bool
	}{
		{
			name:        "a multi-tenant collection is skipped",
			collections: []collection{{class: "PayrollRecords", multiTenant: true, holds: id}},
		},
		{
			name: "a single-tenant miss next to multi-tenant collections is not found",
			collections: []collection{
				{class: "Plain", holds: otherID},
				{class: "PayrollRecords", multiTenant: true, holds: id},
				{class: "HRCases", multiTenant: true, holds: id},
			},
		},
		{
			name: "a single-tenant hit is found regardless of multi-tenant collections",
			collections: []collection{
				{class: "Plain", holds: id},
				{class: "PayrollRecords", multiTenant: true, holds: id},
				{class: "HRCases", multiTenant: true, holds: id},
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			db := &DB{logger: logger, indices: map[string]*Index{}}
			for _, c := range tc.collections {
				db.indices[indexID(schema.ClassName(c.class))] = objectsByIDTestIndex(t, c.class, c.multiTenant, c.holds)
			}
			for range 20 {
				ok, err := db.Exists(t.Context(), "", id, nil, "")
				require.NoError(t, err)
				assert.Equal(t, tc.want, ok)
			}
		})
	}
}

func TestExistsWithoutClassFailsOnUnreadableCollection(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	logger, hook := test.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}
	db.indices[indexID("SecretPlain")] = objectsByIDTestIndex(t, "SecretPlain", false, id)
	require.NoError(t, db.indices[indexID("SecretPlain")].ForEachShard(func(_ string, shard ShardLike) error {
		return shard.Shutdown(t.Context())
	}))

	_, err := db.Exists(t.Context(), "", id, nil, "")
	require.ErrorIs(t, err, errAlreadyShutdown)
	assert.NotContains(t, strings.ToLower(err.Error()), "secretplain")
	require.NotNil(t, hook.LastEntry())
	assert.Equal(t, "SecretPlain", hook.LastEntry().Data["collection"])
}
