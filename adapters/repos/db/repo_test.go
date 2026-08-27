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
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
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

// listAnswer holds what LocalIndexClassNames or GetLocalShardNames returned, sent
// out of the goroutine that called it.
type listAnswer struct {
	names []string
	err   error
}

// callParkedOnIndexLock starts list and returns once it waits for indexLock inside
// DB.<method>. A sleep cannot prove the call got past the entry check.
func callParkedOnIndexLock(t *testing.T, method string, list func() ([]string, error)) <-chan listAnswer {
	t.Helper()

	done := make(chan listAnswer, 1)
	caller := make(chan string, 1)
	go func() {
		var self [64]byte
		caller <- strings.Fields(string(self[:runtime.Stack(self[:], false)]))[1]
		names, err := list()
		done <- listAnswer{names: names, err: err}
	}()

	// Match only this call's goroutine, since a row that failed earlier can
	// leave one parked on another DB's indexLock.
	want := "goroutine " + <-caller + " ["
	buf := make([]byte, 1<<20)
	var last string
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf)) // a truncated dump reads as never parked
			continue
		}
		for _, g := range strings.Split(string(buf[:n]), "\n\n") {
			if !strings.HasPrefix(g, want) {
				continue
			}
			if strings.Contains(g, ".(*DB)."+method+"(") &&
				strings.Contains(g, "RWMutex).RLock(") {
				return done
			}
			last = g
		}
		if time.Now().After(deadline) {
			t.Fatalf("the call never parked on indexLock, last seen at:\n%s", last)
		}
	}
}

func TestLocalIndexClassNames(t *testing.T) {
	dbWithIndices := func(t *testing.T, classNames ...string) *DB {
		t.Helper()

		db := &DB{indices: map[string]*Index{}, shutdown: make(chan struct{})}
		for _, name := range classNames {
			idx := newTestIndex(t, nil, name, nil, nil)
			idx.Config.RootPath = t.TempDir()
			db.indices[indexID(idx.Config.ClassName)] = idx
		}
		return db
	}

	tests := []struct {
		name    string
		classes []string
		want    []string
	}{
		{name: "no indices", want: []string{}},
		{
			// Go randomises map order, and the listed order is no rotation of the
			// sorted one, so an implementation that never sorts fails this row.
			name:    "several indices come back sorted",
			classes: []string{"zeta:Product", "alpha:Product", "Movie"},
			want:    []string{"Movie", "alpha:Product", "zeta:Product"},
		},
		{
			// A schema read matches the name exactly and reports a lowercased one as not found.
			name:    "a name keeps its case",
			classes: []string{"MyNs:MyCollection"},
			want:    []string{"MyNs:MyCollection"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dbWithIndices(t, tc.classes...).LocalIndexClassNames()

			require.NoError(t, err)
			require.NotNil(t, got, "a caller ranging over this needs no nil check")
			assert.Equal(t, tc.want, got)
		})
	}

	// forEachIndexOutsideIndexLock skips a closed index, so a caller iterating with
	// it would miss a collection. This accessor lists one beginClose already closed.
	t.Run("a closed index is still listed", func(t *testing.T) {
		db := dbWithIndices(t, "Product")
		for _, idx := range db.indices {
			require.NoError(t, idx.beginClose())
		}

		got, err := db.LocalIndexClassNames()

		require.NoError(t, err)
		assert.Equal(t, []string{"Product"}, got)
	})

	// Holding indexLock is what makes this row fail without the entry check.
	t.Run("it refuses a stop already under way without waiting for the lock", func(t *testing.T) {
		db := dbWithIndices(t, "Product")
		close(db.shutdown)

		db.indexLock.Lock()
		defer db.indexLock.Unlock()

		done := make(chan listAnswer, 1)
		go func() {
			names, err := db.LocalIndexClassNames()
			done <- listAnswer{names: names, err: err}
		}()

		select {
		case got := <-done:
			require.ErrorIs(t, got.err, ErrIndexClosing)
			require.ErrorIs(t, got.err, errIndexShutdown)
			assert.Nil(t, got.names, "a refusal carries no names a caller could diff against")
		case <-time.After(2 * time.Second):
			t.Fatal("queued behind indexLock instead of answering from db.shutdown")
		}
	})

	// Once the call parks on indexLock it is past the entry check, so only the
	// second check can refuse it.
	t.Run("it refuses a stop that begins while it is parked", func(t *testing.T) {
		db := dbWithIndices(t, "Product")

		db.indexLock.Lock()
		done := callParkedOnIndexLock(t, "LocalIndexClassNames", db.LocalIndexClassNames)

		close(db.shutdown)
		db.indexLock.Unlock()

		select {
		case got := <-done:
			require.ErrorIs(t, got.err, ErrIndexClosing)
			require.ErrorIs(t, got.err, errIndexShutdown)
			assert.Nil(t, got.names, "the list describes a node whose indices are all closed")
		case <-time.After(5 * time.Second):
			t.Fatal("still blocked after indexLock was released")
		}
	})

	// The rows above close db.shutdown themselves. This one checks that DB.Shutdown
	// closes it before taking indexLock, which the entry check relies on.
	t.Run("it refuses a real stop that has not yet taken indexLock", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		db := newShutdownTestDB(t, logger, 0)

		db.indexLock.Lock()
		stopped := make(chan error, 1)
		go func() { stopped <- db.Shutdown(context.Background()) }()

		select {
		case <-db.shutdown:
		case <-time.After(5 * time.Second):
			t.Error("Shutdown took indexLock before closing db.shutdown")
		}

		done := make(chan listAnswer, 1)
		go func() {
			names, err := db.LocalIndexClassNames()
			done <- listAnswer{names: names, err: err}
		}()

		var got listAnswer
		select {
		case got = <-done:
		case <-time.After(2 * time.Second):
			t.Error("queued behind the stop instead of answering from db.shutdown")
		}

		db.indexLock.Unlock()
		require.NoError(t, <-stopped)
		require.ErrorIs(t, got.err, errIndexShutdown)
		assert.Nil(t, got.names, "a refusal carries no names a caller could diff against")
	})
}

func TestGetLocalShardNames(t *testing.T) {
	dbWithShards := func(t *testing.T, collection string, shardNames ...string) (*DB, *Index) {
		t.Helper()

		logger, _ := test.NewNullLogger()
		shards := make(map[string]ShardLike, len(shardNames))
		for _, name := range shardNames {
			// GetLocalShardNames reads only the shard names, so these mocks expect no call.
			shards[name] = NewMockShardLike(t)
		}
		idx := newTestIndex(t, logger, collection, nil, shards)
		idx.Config.RootPath = t.TempDir()

		return &DB{
			indices:  map[string]*Index{indexID(idx.Config.ClassName): idx},
			shutdown: make(chan struct{}),
		}, idx
	}

	t.Run("it lists every resident shard", func(t *testing.T) {
		db, _ := dbWithShards(t, "Product", "s1", "s2")

		got, err := db.GetLocalShardNames("Product")

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "s2"}, got)
	})

	// A shutdown variant and a drop variant would run identical code, because
	// enterRead refuses on i.closed alone and never reads the cause.
	t.Run("a closing index refuses", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		idx.signalCloseRequested(errIndexDropped)
		require.NoError(t, idx.beginClose())

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		// errAlreadyShutdown is the half of the double %w nothing else holds.
		// Collapsing the wrap to "%w: %v" keeps every other row here passing.
		require.ErrorIs(t, err, errAlreadyShutdown)
		// The enterRead refusal carries the cause too.
		require.ErrorIs(t, err, errIndexDropped)
		require.ErrorContains(t, err, `"Product"`,
			"an operator reading one line needs the collection it was asked about")
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")
	})

	// The delete has not reached beginClose, so ForEachShard walks every shard and
	// only the check after the walk refuses.
	t.Run("an index committed for deletion refuses after collecting names", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1", "s2")
		idx.signalCloseRequested(errIndexDropped)

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		require.ErrorIs(t, err, errIndexDropped)
		require.NotErrorIs(t, err, errAlreadyShutdown)
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")
	})

	// ForEachLoadedShard would skip this never-loaded shard.
	t.Run("it lists a resident shard that has never loaded", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		idx.shards.Store("cold", &LazyLoadShard{})

		got, err := db.GetLocalShardNames("Product")

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "cold"}, got)
	})

	// A schema change holds indexLock while DB.Shutdown begins, so DB.GetIndex returns
	// an open index and only the check after the walk can refuse.
	t.Run("a node stop that begins during the lookup refuses", func(t *testing.T) {
		db, _ := dbWithShards(t, "Product", "s1")

		db.indexLock.Lock()
		done := callParkedOnIndexLock(t, "GetLocalShardNames", func() ([]string, error) {
			return db.GetLocalShardNames("Product")
		})

		close(db.shutdown)
		db.indexLock.Unlock()

		select {
		case got := <-done:
			require.ErrorIs(t, got.err, ErrIndexClosing)
			require.ErrorIs(t, got.err, errIndexShutdown)
			assert.Nil(t, got.names, "a refusal carries no names a caller could diff against")
		case <-time.After(5 * time.Second):
			t.Fatal("still blocked after indexLock was released")
		}
	})

	// The node stop has to be answered before db.GetIndex, which parks on
	// indexLock for the whole of DB.Shutdown's close loop.
	t.Run("a node stopping refuses before it looks the index up", func(t *testing.T) {
		db, _ := dbWithShards(t, "Product", "s1")
		close(db.shutdown)

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		require.ErrorIs(t, err, errIndexShutdown)
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")
	})

	t.Run("a collection with no index is not found", func(t *testing.T) {
		db, _ := dbWithShards(t, "Product", "s1")

		_, err := db.GetLocalShardNames("Absent")

		require.ErrorContains(t, err, `collection "Absent" not found`)
		require.NotErrorIs(t, err, ErrIndexClosing)
	})

	// A skipped exitRead passes every other row but leaves beginClose hanging in
	// inflight.Wait.
	t.Run("the index is still closable after a successful call", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		_, err := db.GetLocalShardNames("Product")
		require.NoError(t, err)

		closed := make(chan error, 1)
		go func() { closed <- idx.beginClose() }()

		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("beginClose never drained: the read was entered and not exited")
		}
	})
}
