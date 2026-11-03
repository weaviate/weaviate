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

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// TestObjectsByID pins which collections ObjectsByID searches. Every collection
// holds the same id, so the results name each collection a lookup searched.
func TestObjectsByID(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	logger, _ := test.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}
	for _, c := range []struct {
		class       string
		multiTenant bool
	}{
		{class: "Plain"},
		{class: "Tenanted", multiTenant: true},
		{class: "ns1:Plain"},
		{class: "ns2:Plain"},
		{class: "ns4:Closed"},
	} {
		db.indices[indexID(schema.ClassName(c.class))] = objectsByIDTestIndex(t, c.class, c.multiTenant, id)
	}
	require.NoError(t, db.indices[indexID("ns4:Closed")].ForEachShard(func(_ string, shard ShardLike) error {
		return shard.Shutdown(t.Context())
	}))

	cases := []struct {
		name      string
		namespace string
		want      []string // the classes the lookup finds the id in
		wantErr   error
	}{
		{
			name: "a lookup without a tenant skips a multi-tenant collection",
			want: []string{"Plain"},
		},
		{
			name:      "a namespace searches only its own collections",
			namespace: "ns1",
			want:      []string{"ns1:Plain"},
		},
		{
			name:      "another namespace searches only its own collections",
			namespace: "ns2",
			want:      []string{"ns2:Plain"},
		},
		{
			name:      "a namespace without collections finds nothing",
			namespace: "ns3",
		},
		{
			name:      "a collection that cannot be read fails the lookup",
			namespace: "ns4",
			wantErr:   errAlreadyShutdown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := db.ObjectsByID(t.Context(), id, nil, additional.Properties{}, "", tc.namespace)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			var got []string
			for _, res := range results {
				got = append(got, res.ClassName)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// objectsByIDTestIndex returns a one-shard index of className holding id. It
// carries the shard resolver and namespace lookup Index.objectByID consults.
func objectsByIDTestIndex(t *testing.T, className string, multiTenant bool, id strfmt.UUID) *Index {
	t.Helper()

	exister := namespaces.NewMockExister(t)
	exister.EXPECT().GetNamespace(mock.Anything).
		Return(api.Namespace{State: api.NamespaceStateActive}, true).Maybe()
	withLookups := func(i *Index) {
		i.namespace = namespacing.NamespaceFromQualified(className)
		i.namespacesExister = exister
		i.shardResolver = resolver.NewShardResolver(className, multiTenant, i.getSchema)
	}

	var shard ShardLike
	var idx *Index
	if multiTenant {
		shard, idx = testShardMultiTenant(t, t.Context(), className, withLookups)
	} else {
		shard, idx = testShard(t, t.Context(), className, withLookups)
	}
	obj := testObject(className)
	obj.Object.ID = id
	require.NoError(t, shard.PutObject(t.Context(), obj))
	return idx
}

// TestAnyExists pins that a lookup by id without a class skips multi-tenant
// collections instead of failing at the first one it visits. The schema holds
// several multi-tenant collections so map iteration reaches one before the
// plain collection on almost every run.
func TestAnyExists(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	tenantOnlyID := strfmt.UUID("6b0f2d3c-1e4a-4f7b-8c9d-8e2f3a4b5c6d")
	missingID := strfmt.UUID("7c1a3e4d-2f5b-4a8c-9dae-9f3a4b5c6d7e")
	logger, _ := test.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}
	db.indices[indexID("Plain")] = objectsByIDTestIndex(t, "Plain", false, id)
	for _, class := range []string{"TenantedA", "TenantedB", "TenantedC", "TenantedD"} {
		db.indices[indexID(schema.ClassName(class))] = objectsByIDTestIndex(t, class, true, tenantOnlyID)
	}

	cases := []struct {
		name string
		id   strfmt.UUID
		want bool
	}{
		{name: "an id in a plain collection exists", id: id, want: true},
		{name: "an id held only by multi-tenant collections is not found", id: tenantOnlyID},
		{name: "a missing id is not found", id: missingID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for range 20 {
				ok, err := db.Exists(t.Context(), "", tc.id, nil, "")
				require.NoError(t, err)
				assert.Equal(t, tc.want, ok)
			}
		})
	}
}
