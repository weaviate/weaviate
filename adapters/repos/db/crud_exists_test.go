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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/multi"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Repeats because Go map order varies per walk.
func TestExistsWithoutClass(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	otherID := strfmt.UUID("0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e")

	type collection struct {
		class       string
		multiTenant bool
		holds       strfmt.UUID
		unreadable  bool  // its shards are shut down
		closeCause  error // signalled as the index's close request
		schemaGone  bool  // its class left the local schema, as a delete does first
		cancels     bool  // a read cancels the lookup, then misses
	}
	cases := []struct {
		name        string
		collections []collection
		cancelled   bool
		want        bool
		wantErr     error
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
		{
			name: "a hit among many collections is found",
			collections: []collection{
				{class: "A", holds: otherID},
				{class: "B", holds: otherID},
				{class: "C", holds: otherID},
				{class: "D", holds: otherID},
				{class: "E", holds: id},
				{class: "F", holds: otherID},
			},
			want: true,
		},
		{
			name: "a collection that cannot be read fails a miss",
			collections: []collection{
				{class: "Broken", holds: otherID, unreadable: true},
				{class: "Plain", holds: otherID},
			},
			wantErr: errAlreadyShutdown,
		},
		{
			name: "a collection being deleted is left out",
			collections: []collection{
				{class: "Dropping", holds: id, unreadable: true, closeCause: errIndexDropped},
				{class: "Plain", holds: otherID},
			},
		},
		{
			name: "a collection whose class left the schema is left out",
			collections: []collection{
				{class: "Dropping", holds: id, unreadable: true, schemaGone: true},
				{class: "Plain", holds: otherID},
			},
		},
		{
			name: "a collection shutting down fails the lookup",
			collections: []collection{
				{class: "Closing", holds: id, unreadable: true, closeCause: errIndexShutdown},
				{class: "Plain", holds: otherID},
			},
			wantErr: errAlreadyShutdown,
		},
		{
			name:        "a cancelled lookup fails rather than report not found",
			collections: []collection{{class: "Plain", holds: id}},
			cancelled:   true,
			wantErr:     context.Canceled,
		},
		{
			name:      "a cancelled lookup without collections fails",
			cancelled: true,
			wantErr:   context.Canceled,
		},
		{
			name:        "a cancelled lookup over only multi-tenant collections fails",
			collections: []collection{{class: "PayrollRecords", multiTenant: true, holds: id}},
			cancelled:   true,
			wantErr:     context.Canceled,
		},
		{
			name:        "a lookup cancelled during a miss fails",
			collections: []collection{{class: "Plain", holds: otherID, cancels: true}},
			wantErr:     context.Canceled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			logger, _ := test.NewNullLogger()
			db := &DB{logger: logger, indices: map[string]*Index{}}
			for _, c := range tc.collections {
				idx := objectsByIDTestIndex(t, c.class, c.multiTenant, c.holds)
				if c.cancels {
					var names []string
					require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
						names = append(names, name)
						return nil
					}))
					for _, name := range names {
						idx.shards.Store(name, cancelOnExistsShard{ShardLike: idx.shards.Load(name), cancel: cancel})
					}
				}
				if c.unreadable {
					require.NoError(t, idx.ForEachShard(func(_ string, shard ShardLike) error {
						return shard.Shutdown(t.Context())
					}))
				}
				if c.closeCause != nil {
					idx.signalCloseRequested(c.closeCause)
				}
				if c.schemaGone {
					idx.getSchema = withoutClass{idx.getSchema}
				}
				db.indices[indexID(schema.ClassName(c.class))] = idx
			}
			for range 20 {
				ok, err := db.Exists(ctx, "", id, nil, "")
				if tc.wantErr != nil {
					require.ErrorIs(t, err, tc.wantErr)
					continue
				}
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

// A failure starts no further lookup, so an outage costs one round of timeouts.
func TestExistsWithoutClassStopsAfterAFailure(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	logger, _ := test.NewNullLogger()
	db := &DB{logger: logger, indices: map[string]*Index{}}
	var reads atomic.Int64
	total := 2*_NUMCPU + 1
	for i := range total {
		class := fmt.Sprintf("Broken%d", i)
		idx := objectsByIDTestIndex(t, class, false, id)
		var names []string
		require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
			names = append(names, name)
			return nil
		}))
		for _, name := range names {
			idx.shards.Store(name, failingExistsShard{ShardLike: idx.shards.Load(name), reads: &reads})
		}
		db.indices[indexID(schema.ClassName(class))] = idx
	}

	_, err := db.Exists(t.Context(), "", id, nil, "")
	require.ErrorIs(t, err, errAlreadyShutdown)
	assert.LessOrEqual(t, reads.Load(), int64(_NUMCPU), "only lookups already running may read")
}

// Both lookups are held until both run: a hit beats a running failure, and
// never cancels a running lookup, as a cancelled replica read logs as failed.
func TestExistsWithoutClassWithTwoRunningLookups(t *testing.T) {
	if _NUMCPU < 2 {
		t.Skip("needs two lookups in flight")
	}
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	otherID := strfmt.UUID("0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e")

	for _, other := range []barrierRole{roleFail, roleWatch} {
		t.Run(string(other), func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			db := &DB{logger: logger, indices: map[string]*Index{}}
			barrier := &twoReadsBarrier{t: t, all: make(chan struct{})}
			var cancelled atomic.Bool
			for class, role := range map[string]barrierRole{"Hit": roleHit, "Other": other} {
				holds := otherID
				if role == roleHit {
					holds = id
				}
				idx := objectsByIDTestIndex(t, class, false, holds)
				var names []string
				require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
					names = append(names, name)
					return nil
				}))
				for _, name := range names {
					idx.shards.Store(name, barrierShard{
						ShardLike: idx.shards.Load(name), barrier: barrier, role: role, cancelled: &cancelled,
					})
				}
				db.indices[indexID(schema.ClassName(class))] = idx
			}

			ok, err := db.Exists(t.Context(), "", id, nil, "")
			require.NoError(t, err)
			assert.True(t, ok)
			assert.False(t, cancelled.Load(), "a running lookup was cancelled after the hit")
		})
	}
}

type barrierRole string

const (
	roleHit   barrierRole = "a hit"
	roleFail  barrierRole = "a hit beats a running failure"
	roleWatch barrierRole = "a hit leaves a running lookup alone"
)

// twoReadsBarrier releases the reads once both have entered.
type twoReadsBarrier struct {
	t       *testing.T
	entered atomic.Int32
	all     chan struct{}
}

func (b *twoReadsBarrier) wait() {
	if b.entered.Add(1) == 2 {
		close(b.all)
	}
	select {
	case <-b.all:
	case <-time.After(5 * time.Second):
		b.t.Error("the two lookups never ran at once")
	}
}

type barrierShard struct {
	ShardLike
	barrier   *twoReadsBarrier
	role      barrierRole
	cancelled *atomic.Bool
}

func (s barrierShard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	s.barrier.wait()
	switch s.role {
	case roleFail:
		return false, errAlreadyShutdown
	case roleWatch:
		select {
		case <-ctx.Done():
			s.cancelled.Store(true)
		case <-time.After(100 * time.Millisecond):
		}
	case roleHit:
	}
	return s.ShardLike.Exists(ctx, id)
}

type failingExistsShard struct {
	ShardLike
	reads *atomic.Int64
}

func (s failingExistsShard) Exists(context.Context, strfmt.UUID) (bool, error) {
	s.reads.Add(1)
	return false, errAlreadyShutdown
}

// A replicated read re-takes indexLock, so a lookup holding it can deadlock.
func TestClasslessLookupsRunOutsideIndexLock(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")
	otherID := strfmt.UUID("0b1c2d3e-4f5a-4b6c-8d7e-9f0a1b2c3d4e")
	classes := []string{"First", "Second", "Third"}

	cases := []struct {
		name   string
		lookup func(ctx context.Context, db *DB) error
	}{
		{
			name: "exists",
			lookup: func(ctx context.Context, db *DB) error {
				_, err := db.Exists(ctx, "", id, nil, "")
				return err
			},
		},
		{
			name: "objects by id",
			lookup: func(ctx context.Context, db *DB) error {
				_, err := db.ObjectsByID(ctx, id, nil, additional.Properties{}, "", "")
				return err
			},
		},
		{
			name: "multi get",
			lookup: func(ctx context.Context, db *DB) error {
				var query []multi.Identifier
				for _, class := range classes {
					query = append(query, multi.Identifier{ID: string(id), ClassName: class})
				}
				_, err := db.MultiGet(ctx, query, additional.Properties{}, "")
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			db := &DB{logger: logger, indices: map[string]*Index{}}
			probe := &indexLockProbe{t: t, db: db}
			for _, class := range classes {
				// a miss in every collection, so the lookup reads them all
				idx := objectsByIDTestIndex(t, class, false, otherID)
				var names []string
				require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
					names = append(names, name)
					return nil
				}))
				for _, name := range names {
					idx.shards.Store(name, lockProbeShard{ShardLike: idx.shards.Load(name), probe: probe})
				}
				db.indices[indexID(schema.ClassName(class))] = idx
			}

			require.NoError(t, tc.lookup(t.Context(), db))
			assert.Equal(t, int64(len(classes)), probe.reads.Load())
		})
	}
}

// cancelOnExistsShard cancels the lookup, then answers: a local read ignores its context.
type cancelOnExistsShard struct {
	ShardLike
	cancel context.CancelFunc
}

func (s cancelOnExistsShard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	s.cancel()
	return s.ShardLike.Exists(ctx, id)
}

type indexLockProbe struct {
	t     *testing.T
	db    *DB
	mu    sync.Mutex // concurrent probes would see each other's TryLock
	reads atomic.Int64
}

func (p *indexLockProbe) check() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads.Add(1)
	if !p.db.indexLock.TryLock() {
		p.t.Error("a collection was read while db.indexLock was held")
		return
	}
	p.db.indexLock.Unlock()
}

type lockProbeShard struct {
	ShardLike
	probe *indexLockProbe
}

func (s lockProbeShard) Exists(ctx context.Context, id strfmt.UUID) (bool, error) {
	s.probe.check()
	return s.ShardLike.Exists(ctx, id)
}

func (s lockProbeShard) ObjectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, addl additional.Properties,
) (*storobj.Object, error) {
	s.probe.check()
	return s.ShardLike.ObjectByID(ctx, id, props, addl)
}

func (s lockProbeShard) ObjectSearch(ctx context.Context, limit int, filters *filters.LocalFilter,
	keywordRanking *searchparams.KeywordRanking, sort []filters.Sort, cursor *filters.Cursor,
	addl additional.Properties, properties []string,
) ([]*storobj.Object, []float32, error) {
	s.probe.check()
	return s.ShardLike.ObjectSearch(ctx, limit, filters, keywordRanking, sort, cursor, addl, properties)
}

func (s lockProbeShard) MultiObjectByID(ctx context.Context, query []multi.Identifier) ([]*storobj.Object, error) {
	s.probe.check()
	return s.ShardLike.MultiObjectByID(ctx, query)
}

func TestMultiGet(t *testing.T) {
	id := strfmt.UUID("5a9e1c2b-0d3f-4e6a-9b8c-7d1e2f3a4b5c")

	cases := []struct {
		name       string
		unreadable bool
		closeCause error
		schemaGone bool
		cancelled  bool
		wantFound  bool
		wantErr    error
	}{
		{name: "a readable collection returns the object", wantFound: true},
		{name: "a cancelled lookup fails rather than report a miss", cancelled: true, wantErr: context.Canceled},
		{name: "a collection that cannot be read fails the lookup", unreadable: true, wantErr: errAlreadyShutdown},
		{name: "a collection being deleted is left out", unreadable: true, closeCause: errIndexDropped},
		{name: "a collection whose class left the schema is left out", unreadable: true, schemaGone: true},
		{name: "a collection shutting down fails the lookup", unreadable: true, closeCause: errIndexShutdown, wantErr: errAlreadyShutdown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			db := &DB{logger: logger, indices: map[string]*Index{}}
			idx := objectsByIDTestIndex(t, "Target", false, id)
			if tc.unreadable {
				require.NoError(t, idx.ForEachShard(func(_ string, shard ShardLike) error {
					return shard.Shutdown(t.Context())
				}))
			}
			if tc.closeCause != nil {
				idx.signalCloseRequested(tc.closeCause)
			}
			if tc.schemaGone {
				idx.getSchema = withoutClass{idx.getSchema}
			}
			db.indices[indexID("Target")] = idx
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancelled {
				cancel()
			}

			res, err := db.MultiGet(ctx,
				[]multi.Identifier{{ID: string(id), ClassName: "Target"}}, additional.Properties{}, "")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, res, 1)
			assert.Equal(t, tc.wantFound, res[0].ID == id)
		})
	}
}

// withoutClass is the local schema after a delete took the class out, before the drop.
type withoutClass struct{ schemaUC.SchemaGetter }

func (withoutClass) ReadOnlyClass(string) *models.Class { return nil }
