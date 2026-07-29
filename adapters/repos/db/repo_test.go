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
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestGetIndex(t *testing.T) {
	db := testDB(t, t.TempDir(), []*models.Class{}, make(map[string]*sharding.State))

	// empty indices
	db.indices = map[string]*Index{}
	idx := db.GetIndex(schema.ClassName("test1"))
	require.Nil(t, idx)

	// after 20 ms
	go func() {
		time.Sleep(20 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test1": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test1"))
	require.NotNil(t, idx)

	// after 50 ms
	go func() {
		time.Sleep(50 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test2": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test2"))
	require.NotNil(t, idx)

	// after 100 ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices = map[string]*Index{
			"test3": {},
		}
	}()
	idx = db.GetIndex(schema.ClassName("test3"))
	require.NotNil(t, idx)
}

// TestDB_scanStartupProgress covers the subtle counting in scanStartupProgress
// and localShardsToLoad: the schema-derived HOT-local-shard total, discounting
// lazily-loaded shards, and counting eagerly-loaded shards.
func TestDB_scanStartupProgress(t *testing.T) {
	const localNode = "node1"

	stateWith := func(physicals ...sharding.Physical) *sharding.State {
		m := make(map[string]sharding.Physical, len(physicals))
		for _, p := range physicals {
			m[p.Name] = p
		}
		s := &sharding.State{Physical: m}
		s.SetLocalName(localNode)
		return s
	}

	// indexWith builds a db.indices entry holding the given shards. A background
	// closingCtx keeps ForEachShard from short-circuiting.
	indexWith := func(logger logrus.FieldLogger, className string, shards map[string]ShardLike) map[string]*Index {
		idx := &Index{
			Config:     IndexConfig{ClassName: schema.ClassName(className)},
			logger:     logger,
			closingCtx: context.Background(),
		}
		for name, shard := range shards {
			idx.shards.Store(name, shard)
		}
		return map[string]*Index{idx.ID(): idx}
	}

	tests := []struct {
		name       string
		classes    []*models.Class
		states     map[string]*sharding.State
		indices    func(t *testing.T, logger logrus.FieldLogger) map[string]*Index
		wantLoaded int64
		wantTotal  int64
	}{
		{
			name:    "eager class: a loaded shard counts toward loaded and total",
			classes: []*models.Class{{Class: "Eager"}},
			states: map[string]*sharding.State{
				// non-multi-tenant shard: empty status normalises to HOT.
				"Eager": stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{localNode}}),
			},
			indices: func(t *testing.T, logger logrus.FieldLogger) map[string]*Index {
				return indexWith(logger, "Eager", map[string]ShardLike{"s1": NewMockShardLike(t)})
			},
			wantLoaded: 1,
			wantTotal:  1,
		},
		{
			name:    "lazy class: the shard is discounted from total and not counted as loaded",
			classes: []*models.Class{{Class: "Lazy"}},
			states: map[string]*sharding.State{
				"Lazy": stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{localNode}}),
			},
			indices: func(t *testing.T, logger logrus.FieldLogger) map[string]*Index {
				return indexWith(logger, "Lazy", map[string]ShardLike{"s1": &LazyLoadShard{}})
			},
			wantLoaded: 0,
			wantTotal:  0,
		},
		{
			name: "multi-tenant: only HOT local tenants count toward total",
			classes: []*models.Class{
				{Class: "MT", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
			},
			states: map[string]*sharding.State{
				"MT": stateWith(
					sharding.Physical{Name: "hot", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusHOT},
					sharding.Physical{Name: "cold", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusCOLD},
					sharding.Physical{Name: "remote", BelongsToNodes: []string{"node2"}, Status: models.TenantActivityStatusHOT},
				),
			},
			wantLoaded: 0,
			wantTotal:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t, t.TempDir(), tt.classes, tt.states)
			if tt.indices != nil {
				db.indices = tt.indices(t, db.logger)
			}

			loaded, total := db.scanStartupProgress(db.startupClassNames())
			assert.Equal(t, tt.wantLoaded, loaded, "loaded")
			assert.Equal(t, tt.wantTotal, total, "total")
		})
	}
}
