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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// TestLookupsWithoutClass pins that a lookup naming no collection searches
// every collection, and refuses with NAMESPACES_ENABLED.
func TestLookupsWithoutClass(t *testing.T) {
	id := strfmt.UUID("0f6a3c1e-2b4d-4e8f-9a1b-3c5d7e9f1a2b")
	vector := []float32{1, 0, 0}
	newClass := func(name string) *models.Class {
		return &models.Class{
			Class:               name,
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
		}
	}

	plainDB := createTestDatabaseWithClass(t, monitoring.GetMetrics(), newClass("Plain"))
	require.NoError(t, plainDB.PutObject(t.Context(), &models.Object{Class: "Plain", ID: id}, vector, nil, nil, nil, 0))

	exister := namespaces.NewMockExister(t)
	exister.EXPECT().GetNamespace(mock.Anything).Return(api.Namespace{
		HomeNodes: []string{"node1"},
		State:     api.NamespaceStateActive,
	}, true).Maybe()
	nsDB := openTestDatabase(t, t.TempDir(), singleShardState(), monitoring.GetMetrics(),
		func(cfg *Config) { cfg.NamespacesEnabled = true }, exister, newClass("ns1:Plain"))
	t.Cleanup(func() { require.NoError(t, nsDB.Shutdown(context.Background())) })
	require.NoError(t, nsDB.PutObject(t.Context(), &models.Object{Class: "ns1:Plain", ID: id}, vector, nil, nil, nil, 0))

	resultIDs := func(results []search.Result, err error) ([]strfmt.UUID, error) {
		var ids []strfmt.UUID
		for _, res := range results {
			ids = append(ids, res.ID)
		}
		return ids, err
	}
	cases := []struct {
		name   string
		lookup func(t *testing.T, db *DB) ([]strfmt.UUID, error) // the ids the lookup finds
	}{
		{
			name: "Exists without a class",
			lookup: func(t *testing.T, db *DB) ([]strfmt.UUID, error) {
				ok, err := db.Exists(t.Context(), "", id, nil, "")
				if !ok {
					return nil, err
				}
				return []strfmt.UUID{id}, err
			},
		},
		{
			name: "ObjectSearch",
			lookup: func(t *testing.T, db *DB) ([]strfmt.UUID, error) {
				return resultIDs(db.ObjectSearch(t.Context(), 0, 10, nil, nil, additional.Properties{}, ""))
			},
		},
		{
			name: "CrossClassVectorSearch",
			lookup: func(t *testing.T, db *DB) ([]strfmt.UUID, error) {
				return resultIDs(db.CrossClassVectorSearch(t.Context(), vector, "", 0, 10, nil))
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			found, err := tc.lookup(t, plainDB)
			require.NoError(t, err)
			assert.Equal(t, []strfmt.UUID{id}, found)

			found, err = tc.lookup(t, nsDB)
			require.ErrorIs(t, err, errLookupWithoutClass)
			assert.Empty(t, found)
		})
	}
}
