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
	"context"
	"errors"
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
		{class: "ns5:Dropping"},
		{class: "ns5:Plain"},
		{class: "ns6:Closing"},
		{class: "ns7:Gone"},
	} {
		db.indices[indexID(schema.ClassName(c.class))] = objectsByIDTestIndex(t, c.class, c.multiTenant, id)
	}
	for class, cause := range map[string]error{"ns4:Closed": nil, "ns5:Dropping": errIndexDropped, "ns6:Closing": errIndexShutdown, "ns7:Gone": nil} {
		idx := db.indices[indexID(schema.ClassName(class))]
		require.NoError(t, idx.ForEachShard(func(_ string, shard ShardLike) error {
			return shard.Shutdown(t.Context())
		}))
		if cause != nil {
			idx.signalCloseRequested(cause)
		}
	}
	db.indices[indexID("ns7:Gone")].getSchema = withoutClass{db.indices[indexID("ns7:Gone")].getSchema}

	cases := []struct {
		name      string
		namespace string
		want      []string // the classes the lookup finds the id in
		cancelled bool
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
		{
			name:      "a collection being deleted is left out",
			namespace: "ns5",
			want:      []string{"ns5:Plain"},
		},
		{
			name:      "a collection whose class left the schema is left out",
			namespace: "ns7",
		},
		{
			name:      "a collection shutting down fails the lookup",
			namespace: "ns6",
			wantErr:   errAlreadyShutdown,
		},
		{
			name:      "a cancelled lookup fails rather than report a miss",
			namespace: "ns3",
			cancelled: true,
			wantErr:   context.Canceled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			results, err := db.ObjectsByID(ctx, id, nil, additional.Properties{}, "", tc.namespace)
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

func TestObjectsByIDWithClassCheck(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	logger, _ := test.NewNullLogger()

	t.Run("checks each eligible collection", func(t *testing.T) {
		db := &DB{logger: logger, indices: map[string]*Index{}}
		for _, c := range []struct {
			class       string
			multiTenant bool
		}{
			{class: "Plain"},
			{class: "Other"},
			{class: "Tenanted", multiTenant: true},
			{class: "ns1:Plain"},
			{class: "ns1:Other"},
			{class: "ns1:Tenanted", multiTenant: true},
		} {
			db.indices[indexID(schema.ClassName(c.class))] = objectsByIDTestIndex(t, c.class, c.multiTenant, id)
		}

		for _, tc := range []struct {
			name      string
			namespace string
			want      []string
		}{
			{name: "global", want: []string{"Plain", "Other"}},
			{name: "namespace", namespace: "ns1", want: []string{"ns1:Plain", "ns1:Other"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var checked []string
				results, err := db.ObjectsByIDWithClassCheck(t.Context(), id, nil, additional.Properties{}, "", tc.namespace,
					func(class string) (bool, error) {
						checked = append(checked, class)
						return true, nil
					})
				require.NoError(t, err)
				var found []string
				for _, result := range results {
					found = append(found, result.ClassName)
				}
				assert.ElementsMatch(t, tc.want, found)
				assert.ElementsMatch(t, tc.want, checked)
			})
		}
	})

	t.Run("a skipped collection is not read", func(t *testing.T) {
		db := &DB{logger: logger, indices: map[string]*Index{}}
		db.indices[indexID("Closed")] = objectsByIDTestIndex(t, "Closed", false, id)
		require.NoError(t, db.indices[indexID("Closed")].ForEachShard(func(_ string, shard ShardLike) error {
			return shard.Shutdown(t.Context())
		}))

		results, err := db.ObjectsByIDWithClassCheck(t.Context(), id, nil, additional.Properties{}, "", "",
			func(class string) (bool, error) {
				assert.Equal(t, "Closed", class)
				return false, nil
			})
		require.NoError(t, err)
		require.Empty(t, results)
	})

	t.Run("a failed check prevents the collection read", func(t *testing.T) {
		db := &DB{logger: logger, indices: map[string]*Index{}}
		db.indices[indexID("Closed")] = objectsByIDTestIndex(t, "Closed", false, id)
		require.NoError(t, db.indices[indexID("Closed")].ForEachShard(func(_ string, shard ShardLike) error {
			return shard.Shutdown(t.Context())
		}))
		denied := errors.New("denied")

		_, err := db.ObjectsByIDWithClassCheck(t.Context(), id, nil, additional.Properties{}, "", "",
			func(class string) (bool, error) {
				assert.Equal(t, "Closed", class)
				return false, denied
			})
		require.ErrorIs(t, err, denied)
		require.NotErrorIs(t, err, errAlreadyShutdown)
	})
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
