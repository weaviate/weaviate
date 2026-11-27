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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestGetIndex(t *testing.T) {
	const class = "Article"
	registered := &Index{}

	tests := []struct {
		name string
		// indices is db.indices when GetIndex is called; afterWait replaces it
		// once the wait returns.
		indices   map[string]*Index
		afterWait map[string]*Index
		// version is the class version in the local schema, 0 for a class it
		// does not hold. waitErr is what waiting for it returns.
		version uint64
		waitErr error
		want    *Index
	}{
		{
			name:    "a registered index needs no schema lookup",
			indices: map[string]*Index{"article": registered},
			want:    registered,
		},
		{
			name: "a class the local schema does not hold is missing at once",
		},
		{
			name:      "a class being applied is found once its version is applied",
			version:   7,
			afterWait: map[string]*Index{"article": registered},
			want:      registered,
		},
		{
			name:    "a class whose apply registered no index is missing",
			version: 7,
		},
		{
			name:    "a failed wait reads as missing",
			version: 7,
			waitErr: errors.New("wait timed out"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			schemaReader := schemaUC.NewMockSchemaReader(t)
			db := &DB{logger: logger, indices: tt.indices, schemaReader: schemaReader}
			if tt.indices == nil {
				db.indices = map[string]*Index{}
				schemaReader.EXPECT().ReadOnlyVersionedClass(class).
					Return(versioned.Class{Version: tt.version}).Once()
			}
			if tt.version > 0 {
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, tt.version).
					RunAndReturn(func(context.Context, uint64) error {
						if tt.afterWait != nil {
							db.indexLock.Lock()
							db.indices = tt.afterWait
							db.indexLock.Unlock()
						}
						return tt.waitErr
					}).Once()
			}

			require.Same(t, tt.want, db.GetIndex(class))
		})
	}
}

// TestIndexForIncoming checks a read another node sent waits once, for the
// newer of the asker's class version and this node's own, and that an index
// this node has still answers when that wait fails.
func TestIndexForIncoming(t *testing.T) {
	const class = "Article"
	errWait := errors.New("wait timed out")

	tests := []struct {
		name string
		// registered: this node has the index from the start; registerOnWait:
		// the wait's apply registers it.
		registered, registerOnWait bool
		localVersion, askerVersion uint64
		waitErr                    error
		wantWaitFor                uint64 // 0: no wait
		wantIndex                  bool
		wantErr                    error
	}{
		{
			name: "a node up to date on the class answers", registered: true,
			localVersion: 7, askerVersion: 7, wantWaitFor: 7, wantIndex: true,
		},
		{
			name: "a node without the class waits for the asker's version", registerOnWait: true,
			askerVersion: 9, wantWaitFor: 9, wantIndex: true,
		},
		{
			name: "a node behind on the class waits for the asker's version", registered: true,
			localVersion: 5, askerVersion: 9, wantWaitFor: 9, wantIndex: true,
		},
		{
			name: "a node stuck behind a slow apply still answers from its index", registered: true,
			localVersion: 5, askerVersion: 9, waitErr: errWait, wantWaitFor: 9, wantIndex: true,
		},
		{
			name:         "a node without the class reports a failed wait",
			askerVersion: 9, waitErr: errWait, wantWaitFor: 9, wantErr: errWait,
		},
		{
			// Waiting for the asker's version and then for its own was two waits.
			name:         "a class this node is still creating waits once, for its own newer version",
			localVersion: 9, askerVersion: 7, waitErr: errWait, wantWaitFor: 9, wantErr: errWait,
		},
		{
			name: "an older asker sends no version and nothing waits",
		},
		{
			name: "an older asker still waits for a class being created here", registerOnWait: true,
			localVersion: 9, wantWaitFor: 9, wantIndex: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := &Index{}
			schemaReader := schemaUC.NewMockSchemaReader(t)
			db := &DB{indices: map[string]*Index{}, schemaReader: schemaReader}
			if tt.registered {
				db.indices["article"] = index
			}
			schemaReader.EXPECT().ReadOnlyVersionedClass(class).
				Return(versioned.Class{Version: tt.localVersion}).Once()
			if tt.wantWaitFor > 0 {
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, tt.wantWaitFor).
					RunAndReturn(func(context.Context, uint64) error {
						if tt.registerOnWait {
							db.indexLock.Lock()
							db.indices["article"] = index
							db.indexLock.Unlock()
						}
						return tt.waitErr
					}).Once()
			}

			got, err := db.indexForIncoming(t.Context(), class, tt.askerVersion)

			require.ErrorIs(t, err, tt.wantErr)
			if tt.wantIndex {
				require.Same(t, index, got)
			} else {
				require.Nil(t, got)
			}
		})
	}
}

// TestIndexForIncomingReadLogsAFailedWait checks a failed wait with no index
// is logged and reads as a missing index, as in GetIndexForIncomingSharding.
func TestIndexForIncomingReadLogsAFailedWait(t *testing.T) {
	logger, hook := test.NewNullLogger()
	schemaReader := schemaUC.NewMockSchemaReader(t)
	schemaReader.EXPECT().ReadOnlyVersionedClass("Article").Return(versioned.Class{}).Once()
	schemaReader.EXPECT().WaitForUpdate(mock.Anything, uint64(9)).Return(errors.New("wait timed out")).Once()
	db := &DB{logger: logger, indices: map[string]*Index{}, schemaReader: schemaReader}

	require.Nil(t, db.IndexForIncomingRead(t.Context(), "Article", 9))
	require.Len(t, hook.AllEntries(), 1)
	require.Contains(t, hook.LastEntry().Message, "look up index for incoming request")
	require.Contains(t, hook.LastEntry().Message, "wait timed out")
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

	tests := []struct {
		name string
		// storeShards mimics what initAndStoreShards does for this class: it
		// bumps the tallies as each shard is stored. Progress is read from those
		// counters, not from db.indices, so it moves per shard rather than per
		// published index.
		storeShards func(db *DB)
		classes     []*models.Class
		states      map[string]*sharding.State
		wantLoaded  int64
		wantTotal   int64
	}{
		{
			name:    "eager class: a loaded shard counts toward loaded and total",
			classes: []*models.Class{{Class: "Eager"}},
			states: map[string]*sharding.State{
				// non-multi-tenant shard: empty status normalises to HOT.
				"Eager": stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{localNode}}),
			},
			storeShards: func(db *DB) { db.startupShards.eager.Add(1) },
			wantLoaded:  1,
			wantTotal:   1,
		},
		{
			name:    "lazy class: the shard is discounted from total and not counted as loaded",
			classes: []*models.Class{{Class: "Lazy"}},
			states: map[string]*sharding.State{
				"Lazy": stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{localNode}}),
			},
			storeShards: func(db *DB) { db.startupShards.lazy.Add(1) },
			wantLoaded:  0,
			wantTotal:   0,
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
		{
			// The counters are monotonic but total is recomputed from the live
			// schema, so a class dropped mid-load leaves lazy above what the
			// schema still accounts for.
			name:    "counters outliving the schema clamp total at zero",
			classes: []*models.Class{{Class: "Gone"}},
			states: map[string]*sharding.State{
				"Gone": stateWith(sharding.Physical{Name: "s1", BelongsToNodes: []string{localNode}}),
			},
			storeShards: func(db *DB) { db.startupShards.lazy.Add(5) },
			wantLoaded:  0,
			wantTotal:   0,
		},
		{
			name: "partly loaded class: progress advances before the index is published",
			classes: []*models.Class{
				{Class: "MT", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
			},
			states: map[string]*sharding.State{
				"MT": stateWith(
					sharding.Physical{Name: "t1", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusHOT},
					sharding.Physical{Name: "t2", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusHOT},
					sharding.Physical{Name: "t3", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusHOT},
					sharding.Physical{Name: "t4", BelongsToNodes: []string{localNode}, Status: models.TenantActivityStatusHOT},
				),
			},
			// Two of four tenants stored so far and db.indices still empty, as it
			// is until NewIndex returns. Reading db.indices would report 0/4 here
			// for the whole load, then jump to 4/4.
			storeShards: func(db *DB) {
				db.startupShards.eager.Add(2)
			},
			wantLoaded: 2,
			wantTotal:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t, t.TempDir(), tt.classes, tt.states)
			if tt.storeShards != nil {
				tt.storeShards(db)
			}
			require.Empty(t, db.indices, "progress must not depend on published indices")

			loaded, total := db.scanStartupProgress(db.startupClassNames())
			assert.Equal(t, tt.wantLoaded, loaded, "loaded")
			assert.Equal(t, tt.wantTotal, total, "total")
		})
	}
}

// TestDB_scanStartupProgressDuringLoad pins that progress advances while a
// collection's shards load, before its Index is published to db.indices. With
// one multi-tenant collection holding every shard, counting published indices
// reads 0% for the whole load and then jumps to 100%.
func TestDB_scanStartupProgressDuringLoad(t *testing.T) {
	const localNode = "node1"

	hotShards := func(class string, n int) *sharding.State {
		m := make(map[string]sharding.Physical, n)
		for i := range n {
			name := fmt.Sprintf("%s-s%d", class, i)
			m[name] = sharding.Physical{
				Name:           name,
				BelongsToNodes: []string{localNode},
				Status:         models.TenantActivityStatusHOT,
			}
		}
		s := &sharding.State{Physical: m}
		s.SetLocalName(localNode)
		return s
	}

	// step stores more shards, then states the reading expected at that instant.
	type step struct {
		eager, lazy           int64
		wantLoaded, wantTotal int64
	}

	tests := []struct {
		name    string
		classes []*models.Class
		states  map[string]*sharding.State
		steps   []step
	}{
		{
			name: "one multi-tenant collection: progress advances as its tenants load",
			classes: []*models.Class{
				{Class: "MT", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
			},
			states: map[string]*sharding.State{"MT": hotShards("MT", 8)},
			steps: []step{
				{eager: 2, wantLoaded: 2, wantTotal: 8},
				{eager: 2, wantLoaded: 4, wantTotal: 8},
				{eager: 4, wantLoaded: 8, wantTotal: 8},
			},
		},
		{
			name: "one multi-tenant collection: empty tenants leave the total as they are skipped",
			classes: []*models.Class{
				{Class: "MT", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
			},
			states: map[string]*sharding.State{"MT": hotShards("MT", 8)},
			steps: []step{
				// Empty tenants become LazyLoadShards and drop out of the total as
				// that decision is made, rather than all at once at the end.
				{eager: 1, lazy: 3, wantLoaded: 1, wantTotal: 5},
				{eager: 4, wantLoaded: 5, wantTotal: 5},
			},
		},
		{
			name: "many collections: progress advances while later collections still load",
			classes: []*models.Class{
				{Class: "Alpha"}, {Class: "Beta"}, {Class: "Gamma"},
			},
			states: map[string]*sharding.State{
				"Alpha": hotShards("Alpha", 4),
				"Beta":  hotShards("Beta", 4),
				"Gamma": hotShards("Gamma", 4),
			},
			steps: []step{
				{eager: 4, wantLoaded: 4, wantTotal: 12},
				{eager: 2, wantLoaded: 6, wantTotal: 12},
				{eager: 3, wantLoaded: 9, wantTotal: 12},
				{eager: 3, wantLoaded: 12, wantTotal: 12},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t, t.TempDir(), tt.classes, tt.states)

			for i, s := range tt.steps {
				db.startupShards.eager.Add(s.eager)
				db.startupShards.lazy.Add(s.lazy)

				// Still inside NewIndex: nothing published yet.
				require.Empty(t, db.indices, "step %d", i)

				loaded, total := db.scanStartupProgress(db.startupClassNames())
				assert.Equal(t, s.wantLoaded, loaded, "step %d loaded", i)
				assert.Equal(t, s.wantTotal, total, "step %d total", i)
			}
		})
	}
}
