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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
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

	type answer struct {
		names []string
		err   error
	}

	// callParkedOnIndexLock starts the accessor and returns once it is waiting for
	// indexLock inside copyIndices. A sleep would also be satisfied by a goroutine
	// the scheduler never ran, leaving the entry check to answer the stop.
	callParkedOnIndexLock := func(t *testing.T, db *DB) <-chan answer {
		t.Helper()

		done := make(chan answer, 1)
		caller := make(chan string, 1)
		go func() {
			var self [64]byte
			caller <- strings.Fields(string(self[:runtime.Stack(self[:], false)]))[1]
			names, err := db.LocalIndexClassNames()
			done <- answer{names: names, err: err}
		}()

		// This call's goroutine only: a row that failed earlier can leave one
		// parked on another DB's indexLock.
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
				if strings.Contains(g, ".LocalIndexClassNames(") &&
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

	tests := []struct {
		name    string
		classes []string
		want    []string
	}{
		{name: "no indices", want: []string{}},
		{name: "one index", classes: []string{"Product"}, want: []string{"Product"}},
		{
			// Go randomises map order, and the listed order is no rotation of the
			// sorted one, so an implementation that never sorts fails this row.
			name:    "several indices come back sorted",
			classes: []string{"zeta:Product", "alpha:Product", "Movie"},
			want:    []string{"Movie", "alpha:Product", "zeta:Product"},
		},
		{
			// indexID lowercases the map key, so only Config.ClassName keeps the
			// schema's case. DesiredOpenLocalShardNames looks a name up exactly and
			// reports a lowercased one as not found.
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

	// LocalIndexClassNames reads each *Index after copyIndices releases indexLock,
	// so it runs beside the indexLock section of DeleteIndex. The goroutine deletes
	// the map entry rather than calling DeleteIndex, whose drop writes no field read
	// here.
	t.Run("it runs beside a concurrent delete", func(t *testing.T) {
		db := dbWithIndices(t, "Product", "Movie")

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			db.indexLock.Lock()
			delete(db.indices, indexID("Movie"))
			db.indexLock.Unlock()
		}()

		var got answer
		go func() {
			defer wg.Done()
			got.names, got.err = db.LocalIndexClassNames()
		}()
		wg.Wait()

		require.NoError(t, got.err)
		assert.Contains(t, [][]string{{"Movie", "Product"}, {"Product"}}, got.names,
			"the answer is the class set from before or after the delete, never a torn one")
	})

	// Shutdown closes db.shutdown before it takes indexLock, so a caller arriving
	// later answers from the channel. Holding the lock is what fails this row without
	// the entry check.
	t.Run("it refuses a stop already under way without waiting for the lock", func(t *testing.T) {
		db := dbWithIndices(t, "Product")
		close(db.shutdown)

		db.indexLock.Lock()
		defer db.indexLock.Unlock()

		done := make(chan answer, 1)
		go func() {
			names, err := db.LocalIndexClassNames()
			done <- answer{names: names, err: err}
		}()

		select {
		case got := <-done:
			// ErrIndexClosing is the half a caller outside this package can name.
			// GetLocalShardNames wraps it on a node stop too, so one errors.Is
			// covers that refusal from either accessor.
			require.ErrorIs(t, got.err, ErrIndexClosing)
			require.ErrorIs(t, got.err, errIndexShutdown)
			assert.Nil(t, got.names, "a refusal carries no names a caller could diff against")
		case <-time.After(2 * time.Second):
			t.Fatal("queued behind indexLock instead of answering from db.shutdown")
		}
	})

	// DB.Shutdown write-holds indexLock across every index close, and an RWMutex
	// acquire takes no context, so a call landing on a stop waits the whole stop out.
	t.Run("it blocks while indexLock is write-held", func(t *testing.T) {
		db := dbWithIndices(t, "Product")

		db.indexLock.Lock()
		done := callParkedOnIndexLock(t, db)

		db.indexLock.Unlock()
		select {
		case got := <-done:
			require.NoError(t, got.err)
			assert.Equal(t, []string{"Product"}, got.names)
		case <-time.After(5 * time.Second):
			t.Fatal("still blocked after indexLock was released")
		}
	})

	// A call already parked on indexLock cleared the entry check before the stop
	// began, so waiting for it to park is what fails this row without the post-lock
	// check.
	t.Run("it refuses a stop that begins while it is parked", func(t *testing.T) {
		db := dbWithIndices(t, "Product")

		db.indexLock.Lock()
		done := callParkedOnIndexLock(t, db)

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

	// The entry check only beats a stop because DB.Shutdown closes db.shutdown before
	// it takes indexLock. The rows above close it themselves; this one drives Shutdown.
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

		done := make(chan answer, 1)
		go func() {
			names, err := db.LocalIndexClassNames()
			done <- answer{names: names, err: err}
		}()

		var got answer
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
			// These mocks are never touched. shardMap.Range type-asserts
			// the value and hands it to a callback that reads only the key.
			shards[name] = NewMockShardLike(t)
		}
		idx := newTestIndex(t, logger, collection, nil, shards)
		idx.Config.RootPath = t.TempDir()

		return &DB{
			indices:  map[string]*Index{indexID(idx.Config.ClassName): idx},
			shutdown: make(chan struct{}),
		}, idx
	}

	// A shutdown variant and a drop variant would run identical code, because
	// enterRead refuses on i.closed alone and never reads the cause.
	t.Run("a closing index refuses", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		require.NoError(t, idx.beginClose())

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		// errAlreadyShutdown is the half of the double %w nothing else holds.
		// Collapsing the wrap to "%w: %v" keeps every other row here passing.
		require.ErrorIs(t, err, errAlreadyShutdown)
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")
	})

	// ForEachShard returns nil having visited nothing once closeCause is set,
	// so the walk reports no error and no names, and only the re-check refuses.
	t.Run("an index closing before the walk refuses with its cause", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		// signalCloseRequested alone leaves closeCause nil, because closeCause
		// reads closingCtx, whose only other canceller is beginClose. Calling
		// beginClose here instead deadlocks on inflight.Wait against our own
		// enterRead.
		idx.signalCloseRequested(errIndexShutdown)
		idx.closingCancel()

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		require.ErrorIs(t, err, errIndexShutdown)
		require.NotErrorIs(t, err, errAlreadyShutdown)
		require.ErrorContains(t, err, `"Product"`,
			"an operator reading one line needs the collection it was asked about")
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")

		closed := make(chan error, 1)
		go func() { closed <- idx.beginClose() }()

		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("beginClose never drained: a refusal leaked an inflight count")
		}
	})

	// Both arms carry the cause, so a caller in this package can ask why the
	// index is closing without knowing which arm answered. Before beginClose the
	// post-walk arm answers; after it, enterRead does.
	t.Run("the entry refusal carries the cause and the collection", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		idx.signalCloseRequested(errIndexDropped)
		require.NoError(t, idx.beginClose())

		_, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		require.ErrorIs(t, err, errAlreadyShutdown)
		require.ErrorIs(t, err, errIndexDropped)
		require.ErrorContains(t, err, `"Product"`,
			"an operator reading one line needs the collection it was asked about")
	})

	// The non-empty shape: a delete has signalled its cause but not reached
	// beginClose, so closeCause is still nil, ForEachShard walks to completion,
	// and the re-check is the only thing that turns real names into a refusal.
	t.Run("an index committed for deletion refuses after collecting names", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1", "s2")
		idx.signalCloseRequested(errIndexDropped)

		got, err := db.GetLocalShardNames("Product")

		require.ErrorIs(t, err, ErrIndexClosing)
		require.ErrorIs(t, err, errIndexDropped)
		require.NotErrorIs(t, err, errAlreadyShutdown)
		assert.Nil(t, got, "a refusal carries no names a caller could diff against")
	})

	// A resident LazyLoadShard that never loaded is the one shape
	// ForEachLoadedShard filters out. Swapping the walk to it drops this name
	// while the shard map still holds the shard.
	t.Run("it lists a resident shard that has never loaded", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		idx.shards.Store("cold", &LazyLoadShard{})

		got, err := db.GetLocalShardNames("Product")

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "cold"}, got)
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

	// DB.GetIndex hands back nil after its four backoff attempts, so the class
	// is absent on this node rather than closing.
	t.Run("a collection with no index wraps ErrClassNotFound", func(t *testing.T) {
		db, _ := dbWithShards(t, "Product", "s1")

		got, err := db.GetLocalShardNames("Absent")

		require.ErrorIs(t, err, clusterSchema.ErrClassNotFound)
		require.ErrorContains(t, err, `collection "Absent"`,
			"an operator reading one line needs the collection it was asked about")
		assert.Nil(t, got)
	})

	// A single wrapper over both error returns would pass every row above, so
	// each sentinel is asserted not to answer for the other.
	t.Run("the two refusals carry different sentinels", func(t *testing.T) {
		db, idx := dbWithShards(t, "Product", "s1")
		require.NoError(t, idx.beginClose())

		_, closing := db.GetLocalShardNames("Product")
		_, absent := db.GetLocalShardNames("Absent")

		require.NotErrorIs(t, closing, clusterSchema.ErrClassNotFound)
		require.NotErrorIs(t, absent, ErrIndexClosing)
	})

	// A caller diffs this against the shards it should hold, so an empty set
	// has to be an answer rather than a refusal.
	t.Run("it counts 0, 1 and N shards", func(t *testing.T) {
		tests := []struct {
			name   string
			shards []string
		}{
			{name: "no local shards"},
			{name: "one local shard", shards: []string{"s1"}},
			{name: "several local shards", shards: []string{"s1", "s2", "s3"}},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				db, _ := dbWithShards(t, "Product", tc.shards...)

				got, err := db.GetLocalShardNames("Product")

				require.NoError(t, err)
				assert.NotNil(t, got, "a caller ranging over this needs no nil check")
				assert.ElementsMatch(t, tc.shards, got)
			})
		}
	})

	// The six shapes below are every one a caller diffing held shards against
	// desired ones has to see. A torn shard is held and desired at once, so a
	// suspended namespace releases it like any other.
	t.Run("it lists every residency shape", func(t *testing.T) {
		newShard := func() *Shard { return &Shard{shutdownLock: new(sync.RWMutex)} }
		tornShard := func() *Shard {
			s := newShard()
			s.shut.Store(true)
			s.teardownErr = errors.New("bucket close failed")
			return s
		}

		torn, tornLazy := tornShard(), &LazyLoadShard{shard: tornShard(), loaded: true}
		cleanlyShut := newShard()
		cleanlyShut.shut.Store(true)
		require.Error(t, torn.teardownError(), "a torn row means nothing unless the shard is torn")
		require.Error(t, tornLazy.shard.teardownError())
		require.NoError(t, cleanlyShut.teardownError(), "a nil teardownErr is what tells the two apart")

		db, idx := dbWithShards(t, "Product")
		idx.shards.Store("eager", newShard())
		idx.shards.Store("lazyLoaded", &LazyLoadShard{shard: newShard(), loaded: true})
		idx.shards.Store("neverLoaded", &LazyLoadShard{})
		idx.shards.Store("torn", torn)
		idx.shards.Store("tornLazy", tornLazy)
		idx.shards.Store("cleanlyShut", cleanlyShut)

		got, err := db.GetLocalShardNames("Product")

		require.NoError(t, err)
		assert.ElementsMatch(t, []string{
			"eager", "lazyLoaded", "neverLoaded", "torn", "tornLazy", "cleanlyShut",
		}, got)
	})

	// A missing or mis-scoped defer exitRead passes every row above and hangs
	// only the next DeleteClass or SIGTERM, because beginClose ends in
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

// A caller diffing held shards against desired ones pairs the four accessors,
// which is only possible if they agree on the class name's form.
// LocalIndexClassNames is the only one that produces a name, so what it emits
// has to be what the other three accept.
func TestAccessorsAgreeOnOneClassName(t *testing.T) {
	tests := []struct {
		name  string
		class string
	}{
		// Without NAMESPACES_ENABLED every class carries an unqualified name,
		// and NamespaceStateForClass answers active without reaching
		// namespacesExister.
		{name: "namespaces off", class: unqualifiedClass},
		// namespacesExister answers only for a qualified name, so this row is
		// the one that shows the emitted name still carries its namespace.
		{name: "a namespaced class", class: namespacedClass},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			idx := newTestIndex(t, logger, tc.class, nil, map[string]ShardLike{
				"s1": NewMockShardLike(t), "s2": NewMockShardLike(t),
			})
			idx.Config.RootPath = t.TempDir()
			db := &DB{
				logger:   logger,
				indices:  map[string]*Index{indexID(idx.Config.ClassName): idx},
				shutdown: make(chan struct{}),
				schemaReader: readerForShards(t, tc.class, map[string]sharding.Physical{
					"s1": hotPhysical("s1"), "s2": hotPhysical("s2"),
				}),
				namespacesExister: existerWithState(t, api.NamespaceStateActive),
			}

			names, err := db.LocalIndexClassNames()
			require.NoError(t, err)
			require.Equal(t, []string{tc.class}, names)

			// names[0] rather than tc.class, so the three are driven by the name
			// LocalIndexClassNames emitted. DesiredOpenLocalShardNames reads the
			// schema, and that read rejects a lowercased name. The other two
			// resolve one case-insensitively.
			className := names[0]

			state, err := db.NamespaceStateForClass(className)
			require.NoError(t, err)
			assert.Equal(t, api.NamespaceStateActive, state)

			desired, desiredState, err := db.DesiredOpenLocalShardNames(className)
			require.NoError(t, err)
			assert.Equal(t, api.NamespaceStateActive, desiredState)
			assert.ElementsMatch(t, []string{"s1", "s2"}, desired)

			local, err := db.GetLocalShardNames(className)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"s1", "s2"}, local)
		})
	}
}
