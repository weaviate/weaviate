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
	"path/filepath"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication/changelog"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// indexForNamespace builds the minimum Index the guard adapters read: the
// qualified class name they parse the namespace out of, plus the exister they
// look the state up in.
func indexForNamespace(t *testing.T, className string, e namespaces.Exister) (*Index, *logrustest.Hook) {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	return &Index{
		Config: IndexConfig{ClassName: schema.ClassName(className)},
		// the same parse NewIndex does, so these exercise the production value
		namespace:         namespacing.NamespaceFromQualified(className),
		namespacesExister: e,
		logger:            logger,
	}, hook
}

// newIndexForNamespaceTest drives the real NewIndex, so the fields the guard
// adapters read come from the constructor rather than from a literal.
func newIndexForNamespaceTest(t *testing.T, className string, e namespaces.Exister) (*Index, error) {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}

	sg := schemaUC.NewMockSchemaGetter(t)
	sg.On("ReadOnlyClass", className).Return(class).Maybe()
	sg.On("NodeName").Return("node1").Maybe()

	ss := &sharding.State{Physical: map[string]sharding.Physical{}}
	ss.SetLocalName("node1")
	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	idx, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName(className),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		NamespacesExister: e,
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil,
		resolver.NewShardResolver(className, false, sg),
		sg, reader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil,
		memwatch.NewDummyMonitor(),
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = idx.Shutdown(context.Background()) })
	return idx, nil
}

func TestNamespaceGuard(t *testing.T) {
	t.Run("the owning namespace's state reaches the load check", func(t *testing.T) {
		tests := []struct {
			name        string
			state       api.NamespaceState
			loadableErr error
		}{
			{name: "active", state: api.NamespaceStateActive},
			{name: "suspended", state: api.NamespaceStateSuspended, loadableErr: namespaces.ErrNamespaceSuspended},
			{name: "resuming", state: api.NamespaceStateResuming, loadableErr: namespaces.ErrNamespaceResuming},
			{name: "deleting", state: api.NamespaceStateDeleting, loadableErr: namespaces.ErrNamespaceDeleting},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := namespaces.NewMockExister(t)
				e.EXPECT().GetNamespace("alpha").
					Return(api.Namespace{Name: "alpha", State: tc.state}, true)

				idx, _ := indexForNamespace(t, "alpha:Product", e)

				if tc.loadableErr != nil {
					require.ErrorIs(t, idx.requireNamespaceAllowsShardLoad(callerUserRequest), tc.loadableErr)
				} else {
					require.NoError(t, idx.requireNamespaceAllowsShardLoad(callerUserRequest))
				}
			})
		}
	})

	// A namespaced class with no lookup and one whose namespace is absent both
	// refuse, but for reasons an operator must be able to tell apart: the first
	// is a lost wiring line, the second a broken invariant.
	t.Run("a namespaced class refuses on a missing lookup and on a missing namespace", func(t *testing.T) {
		noLookup, noLookupHook := indexForNamespace(t, "alpha:Product", nil)
		require.ErrorIs(t, noLookup.requireNamespaceAllowsShardLoad(callerUserRequest), errNoNamespaceLookup)
		require.NotNil(t, noLookupHook.LastEntry(), "a missing lookup must be logged")

		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		miss, _ := indexForNamespace(t, "alpha:Product", e)

		require.ErrorIs(t, miss.requireNamespaceAllowsShardLoad(callerUserRequest), errNamespaceUnknownLocally)
		require.NotErrorIs(t, miss.requireNamespaceAllowsShardLoad(callerUserRequest), errNoNamespaceLookup)
	})

	// An unqualified class name is the only un-namespaced case, so it is the only
	// one that may skip the lookup and be admitted.
	t.Run("an unqualified class name is admitted with no lookup at all", func(t *testing.T) {
		idx, _ := indexForNamespace(t, "Product", nil)

		require.Empty(t, idx.namespace)
		require.NoError(t, idx.requireNamespaceAllowsShardLoad(callerUserRequest))
	})

	// NewMockExister fails the test on an unexpected call, so registering no
	// expectation asserts an unqualified name never reaches the lookup.
	t.Run("an unqualified class name does not consult the lookup it has", func(t *testing.T) {
		idx, _ := indexForNamespace(t, "Product", namespaces.NewMockExister(t))

		require.NoError(t, idx.requireNamespaceAllowsShardLoad(callerUserRequest))
	})

	t.Run("a lookup miss is logged at Error with class and namespace", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		idx, hook := indexForNamespace(t, "alpha:Product", e)

		require.ErrorIs(t, idx.requireNamespaceAllowsShardLoad(callerUserRequest), errNamespaceUnknownLocally)

		entry := hook.LastEntry()
		require.NotNil(t, entry, "a lookup miss must be logged")
		assert.Equal(t, logrus.ErrorLevel, entry.Level)
		assert.Equal(t, "alpha:Product", entry.Data["class"])
		assert.Equal(t, "alpha", entry.Data["namespace"])
		assert.Contains(t, entry.Message, errNamespaceUnknownLocally.Error())
	})

	// An active namespace admits every caller the switch knows, so a refusal here
	// can only come from the caller.
	t.Run("a caller with no case is refused as unknown, not as a closed namespace", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").
			Return(api.Namespace{Name: "alpha", State: api.NamespaceStateActive}, true)
		idx, _ := indexForNamespace(t, "alpha:Product", e)

		err := idx.requireNamespaceAllowsShardLoad(shardLoadCaller(99))
		require.ErrorIs(t, err, errUnknownShardLoadCaller)
		require.NotErrorIs(t, err, errShardNamespaceClosed)
	})
}

// The guard adapters read two fields NewIndex populates from its config. Built
// directly, an Index in the other tests cannot catch either being dropped there,
// so this drives the real constructor.
func TestNewIndexCarriesNamespaceLookup(t *testing.T) {
	tests := []struct {
		name          string
		className     string
		wantNamespace string
	}{
		{name: "qualified name yields its namespace", className: "alpha:Product", wantNamespace: "alpha"},
		{name: "unqualified name yields none", className: "Product", wantNamespace: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The constructor calls into initAndStoreShards, which looks the state
			// up, so a qualified name consults the exister before the assertions
			// below.
			e := namespaces.NewMockExister(t)
			e.EXPECT().GetNamespace("alpha").
				Return(api.Namespace{Name: "alpha", State: api.NamespaceStateActive}, true).Maybe()
			idx, err := newIndexForNamespaceTest(t, tc.className, e)
			require.NoError(t, err)

			require.Equal(t, tc.wantNamespace, idx.namespace,
				"NewIndex must parse the owning namespace out of the class name")
			require.Same(t, e, idx.namespacesExister,
				"NewIndex must carry the exister from its config")
		})
	}

	// The refusal has to reach the constructor's caller: an index that could not
	// decide whether to open its shards must not come back as a usable one.
	t.Run("a lookup miss fails the constructor", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)

		idx, err := newIndexForNamespaceTest(t, "alpha:Product", e)
		require.ErrorIs(t, err, errNamespaceUnknownLocally)
		assert.Nil(t, idx)
	})
}

// A shard with no status counts as HOT, so a single-tenant shard, which never
// carries one, is decided by its namespace alone. The namespace axis is covered
// by TestDesiredOpenLocalShardCount.
func TestShardStatusOpen(t *testing.T) {
	statuses := []struct {
		name   string
		status string
		hot    bool
	}{
		{name: "HOT", status: models.TenantActivityStatusHOT, hot: true},
		{name: "empty", status: "", hot: true},
		{name: "COLD", status: models.TenantActivityStatusCOLD},
		{name: "FROZEN", status: models.TenantActivityStatusFROZEN},
		{name: "FREEZING", status: models.TenantActivityStatusFREEZING},
		{name: "UNFREEZING", status: models.TenantActivityStatusUNFREEZING},
	}

	for _, st := range statuses {
		t.Run(st.name, func(t *testing.T) {
			assert.Equal(t, st.hot, shardStatusOpen(st.status))
		})
	}
}

// readerForShards serves one class's sharding state to the code under test.
func readerForShards(t *testing.T, className string, shards map[string]sharding.Physical) *schemaUC.MockSchemaReader {
	t.Helper()

	state := reloadState(false, shards)

	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().Read(className, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, state)
		}).Maybe()
	return reader
}

// dbForDesiredOpen builds the minimum DB the desired-open decision reads: a
// schema reader over one class's sharding state, plus the namespace lookup.
func dbForDesiredOpen(t *testing.T, className string, e namespaces.Exister, shards map[string]sharding.Physical) *DB {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	return &DB{
		logger:            logger,
		schemaReader:      readerForShards(t, className, shards),
		namespacesExister: e,
	}
}

// localPhysical carries no activity status, the shape every single-tenant shard
// has and the empty-counts-as-HOT cell for a tenant.
func localPhysical(name string) sharding.Physical {
	return sharding.Physical{Name: name, BelongsToNodes: []string{"node1"}}
}

func hotPhysical(name string) sharding.Physical {
	p := localPhysical(name)
	p.Status = models.TenantActivityStatusHOT
	return p
}

func coldPhysical(name string) sharding.Physical {
	p := localPhysical(name)
	p.Status = models.TenantActivityStatusCOLD
	return p
}

func TestDesiredOpenLocalShardCount(t *testing.T) {
	const class = "alpha:Product"

	tests := []struct {
		name       string
		className  string
		state      api.NamespaceState
		namespaced bool
		shards     map[string]sharding.Physical
		// want names the shards, not just how many, so a swapped inclusion
		// cannot pass on an unchanged total.
		want    []string
		wantErr error
	}{
		{
			name: "a class with no shards at all yields nothing", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{},
		},
		{
			name: "no local shards yields an empty set", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"other"}}},
		},
		{
			// A replica list that never got populated must not read as local, or
			// every node would count the shard toward its own startup progress.
			name: "a shard with no replicas is left out", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{"s1": {Name: "s1"}},
		},
		{
			name: "one HOT shard yields that shard", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{"s1": localPhysical("s1")},
			want:   []string{"s1"},
		},
		{
			// The fixture leaves PartitioningEnabled unset, so this also pins that a
			// non-HOT status closes a single-tenant shard. One is not producible, but
			// the startup load would skip it too.
			name: "an active class yields only its HOT shards", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{
				"hot1": localPhysical("hot1"), "hot2": localPhysical("hot2"), "cold1": coldPhysical("cold1"),
			},
			want: []string{"hot1", "hot2"},
		},
		{
			// other1 and cold1 are both left out, for reasons a caller must not
			// merge: cold1 should be closed, while other1 is simply not this
			// node's to decide. Reading the whole complement as unloadable would
			// take out a shard the answer was never about.
			name: "a shard this node is not a replica of is left out", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			shards: map[string]sharding.Physical{
				"hot1": localPhysical("hot1"), "cold1": coldPhysical("cold1"),
				"other1": {Name: "other1", BelongsToNodes: []string{"node2"}},
			},
			want: []string{"hot1"},
		},
		{
			name: "a suspended class yields nothing", className: class,
			state: api.NamespaceStateSuspended, namespaced: true,
			shards: map[string]sharding.Physical{
				"hot1": localPhysical("hot1"), "hot2": localPhysical("hot2"),
			},
		},
		{
			// Not the empty set: the shards must reopen for the resume to finish.
			name: "a resuming class yields its HOT shards", className: class,
			state: api.NamespaceStateResuming, namespaced: true,
			shards: map[string]sharding.Physical{
				"hot1": localPhysical("hot1"), "cold1": coldPhysical("cold1"),
			},
			want: []string{"hot1"},
		},
		{
			name: "a deleting class yields nothing", className: class,
			state: api.NamespaceStateDeleting, namespaced: true,
			shards: map[string]sharding.Physical{"hot1": localPhysical("hot1")},
		},
		{
			// A state this binary has no case for, which a newer node can write.
			// It must close the shards rather than fall through as active.
			name: "a class in an unrecognized state yields nothing", className: class,
			state: api.NamespaceState("bogus"), namespaced: true,
			shards: map[string]sharding.Physical{"hot1": localPhysical("hot1")},
		},
		{
			name: "an unqualified class yields its HOT shards", className: "Product",
			shards: map[string]sharding.Physical{
				"hot1": localPhysical("hot1"), "cold1": coldPhysical("cold1"),
			},
			want: []string{"hot1"},
		},
		{
			// Not an empty set, which a caller would read as "unload everything".
			name: "a lookup miss returns the error", className: class,
			namespaced: true,
			shards:     map[string]sharding.Physical{"hot1": localPhysical("hot1")},
			wantErr:    errNamespaceUnknownLocally,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e namespaces.Exister
			if tc.namespaced {
				me := namespaces.NewMockExister(t)
				me.EXPECT().GetNamespace("alpha").
					Return(api.Namespace{Name: "alpha", State: tc.state}, tc.state != "").Maybe()
				e = me
			}
			db := dbForDesiredOpen(t, tc.className, e, tc.shards)

			got, err := db.DesiredOpenLocalShardCount(tc.className)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Zero(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, len(tc.want), got)

			var walked []string
			require.NoError(t, db.forEachDesiredOpenLocalShard(tc.className,
				func(name string) { walked = append(walked, name) }))
			sort.Strings(walked)
			assert.Equal(t, tc.want, walked)
		})
	}

	// Registering no Read expectation asserts the shards are never enumerated:
	// when nothing may be open the answer is zero whatever the shards are, and a
	// sweep pass must not walk every tenant to learn that.
	t.Run("a suspended class is answered without reading the sharding state", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").
			Return(api.Namespace{Name: "alpha", State: api.NamespaceStateSuspended}, true)
		logger, _ := logrustest.NewNullLogger()
		db := &DB{logger: logger, schemaReader: schemaUC.NewMockSchemaReader(t), namespacesExister: e}

		got, err := db.DesiredOpenLocalShardCount(class)
		require.NoError(t, err)
		assert.Zero(t, got)
	})
}

// Startup progress polls the count per class every few seconds while the node
// loads, so an allocation per shard lands on a node with many tenants exactly
// when it is busiest. Comparing two shard counts pins that rather than a
// literal, which would only pin today's fixture.
func TestDesiredOpenLocalShardCountDoesNotAllocatePerShard(t *testing.T) {
	const class = "alpha:Product"

	allocsFor := func(shards int) float64 {
		physical := make(map[string]sharding.Physical, shards)
		for i := range shards {
			name := fmt.Sprintf("tenant-%d", i)
			physical[name] = hotPhysical(name)
		}
		db := dbForDesiredOpen(t, class, existerWithState(t, api.NamespaceStateActive), physical)

		return testing.AllocsPerRun(20, func() {
			got, err := db.DesiredOpenLocalShardCount(class)
			if err != nil || got != shards {
				t.Fatalf("count = %d, err = %v", got, err)
			}
		})
	}

	// testing.AllocsPerRun counts process-wide mallocs, so whatever background
	// goroutines the package's other tests leave running land in the measurement
	// too — a few allocations either way. Comparing within a tolerance absorbs
	// that; an allocation per shard would show up as thousands, not units.
	const foreignAllocations = 10
	assert.InDelta(t, allocsFor(100), allocsFor(10_000), foreignAllocations,
		"the walk must not allocate per shard")
}

var errReadFailed = errors.New("read failed")

// A sharding state that cannot be read must surface as an error, not as a count
// of zero — a sweep would take zero as licence to unload the class entirely.
func TestDesiredOpenLocalShardCountReadFailures(t *testing.T) {
	const class = "alpha:Product"

	tests := []struct {
		name    string
		read    func(*schemaUC.MockSchemaReader)
		wantErr error
	}{
		{
			name: "an absent sharding state",
			read: func(r *schemaUC.MockSchemaReader) {
				r.EXPECT().Read(class, mock.Anything, mock.Anything).
					RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
						return readFunc(&models.Class{Class: class}, nil)
					})
			},
		},
		{
			name: "a failing read",
			read: func(r *schemaUC.MockSchemaReader) {
				r.EXPECT().Read(class, mock.Anything, mock.Anything).Return(errReadFailed)
			},
			wantErr: errReadFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			e := namespaces.NewMockExister(t)
			e.EXPECT().GetNamespace("alpha").
				Return(api.Namespace{Name: "alpha", State: api.NamespaceStateActive}, true)

			reader := schemaUC.NewMockSchemaReader(t)
			tc.read(reader)

			db := &DB{logger: logger, schemaReader: reader, namespacesExister: e}

			got, err := db.DesiredOpenLocalShardCount(class)
			require.Error(t, err)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
			assert.Zero(t, got)
		})
	}
}

// indexForGuardTest builds the minimum Index initLocalShardWithForcedLoading
// needs before it reaches the resident-shard branch.
func indexForGuardTest(t *testing.T, className string, e namespaces.Exister) *Index {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	idx := &Index{
		Config:                 IndexConfig{RootPath: t.TempDir(), ClassName: schema.ClassName(className)},
		namespace:              namespacing.NamespaceFromQualified(className),
		namespacesExister:      e,
		logger:                 logger,
		shardCreateLocks:       esync.NewKeyRWLocker(),
		replicaSnapshotOpLocks: esync.NewKeyRWLocker(),
		backupLock:             esync.NewKeyRWLocker(),
		closingCtx:             context.Background(),
	}
	idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
	return idx
}

// namespaceLookupRefusals returns both fail-closed arms of the chokepoint: a
// namespaced class with no lookup at all, and one whose namespace the lookup
// does not hold. Every entry point owes them the same two errors.
func namespaceLookupRefusals() []struct {
	name    string
	exister func(*testing.T) namespaces.Exister
	wantErr error
} {
	return []struct {
		name    string
		exister func(*testing.T) namespaces.Exister
		wantErr error
	}{
		{
			name:    "a missing lookup",
			exister: func(*testing.T) namespaces.Exister { return nil },
			wantErr: errNoNamespaceLookup,
		},
		{
			name: "a lookup miss",
			exister: func(t *testing.T) namespaces.Exister {
				e := namespaces.NewMockExister(t)
				e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false).Maybe()
				return e
			},
			wantErr: errNamespaceUnknownLocally,
		},
	}
}

func existerWithState(t *testing.T, state api.NamespaceState) namespaces.Exister {
	t.Helper()
	e := namespaces.NewMockExister(t)
	e.EXPECT().GetNamespace("alpha").
		Return(api.Namespace{Name: "alpha", State: state}, true).Maybe()
	return e
}

// The guard sits above the resident-shard branch, so a shard already in i.shards
// is refused rather than force-loaded. Seeding a zero-value LazyLoadShard makes
// that observable: reaching the branch would call Load on it, which cannot
// succeed, so a namespace error proves the guard ran first.
func TestGuardLoadPath(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	tests := []struct {
		name     string
		state    api.NamespaceState
		resident bool
		wantErr  error
	}{
		{name: "suspended refuses", state: api.NamespaceStateSuspended, wantErr: namespaces.ErrNamespaceSuspended},
		{name: "deleting refuses", state: api.NamespaceStateDeleting, wantErr: namespaces.ErrNamespaceDeleting},
		{name: "resuming refuses a request load", state: api.NamespaceStateResuming, wantErr: namespaces.ErrNamespaceResuming},
		{
			// Red if the guard is placed beside the initShard call: a resident
			// shard returns before ever reaching it.
			name: "suspended refuses a resident shard", state: api.NamespaceStateSuspended,
			resident: true, wantErr: namespaces.ErrNamespaceSuspended,
		},
		{
			name: "deleting refuses a resident shard", state: api.NamespaceStateDeleting,
			resident: true, wantErr: namespaces.ErrNamespaceDeleting,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, tc.state))
			if tc.resident {
				idx.shards.Store("t1", &LazyLoadShard{})
			}

			err := idx.initLocalShardWithForcedLoading(ctx, &models.Class{Class: class}, "t1", true, false, callerUserRequest)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	t.Run("a lookup miss refuses", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		idx := indexForGuardTest(t, class, e)

		err := idx.initLocalShardWithForcedLoading(ctx, &models.Class{Class: class}, "t1", true, false, callerUserRequest)
		require.ErrorIs(t, err, errNamespaceUnknownLocally)
	})

	// The allow side: without it, a guard that refused every in-band load would
	// pass every refuse-only assertion above.
	t.Run("an active namespace admits the load", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateActive))
		idx.shards.Store("t1", &LazyLoadShard{})

		err := idx.requireNamespaceAllowsShardLoad(callerUserRequest)
		require.NoError(t, err)
	})

	t.Run("an unqualified class name admits the load", func(t *testing.T) {
		idx := indexForGuardTest(t, "Product", nil)

		err := idx.requireNamespaceAllowsShardLoad(callerUserRequest)
		require.NoError(t, err)
	})
}

// A movement reaches its target shard three times — to load it, to replay the
// change log onto it, and once more from the apply that adds the replica to the
// sharding state — and a suspend or resume must fail none of them, while a
// deleting namespace refuses all three. What these rows do not say is that a
// movement can start while suspended: its first source-side call,
// IncomingStartChangeCapture, goes through the request path, which refuses.
//
// Each row drives the entry point itself rather than the guard adapter, so
// routing any of them back through the request path turns it red.
func TestReplicationExempt(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	// A delete needs no encoded payload, so the shard call the replay makes is a
	// single mocked DeleteObject.
	replay := []ChangeLogReplayEntry{{ID: "f8b6a3e0-0000-4000-8000-000000000001", IsDelete: true, LastUpdateTimeUnixMilli: 1}}

	// Seeds the resident shard both entry points reach. A resident non-lazy shard
	// is already open, so the load returns once admitted; the replay writes to it.
	// Expectations are set only where the namespace admits, so an admitted row
	// that never reaches the shard fails on the unmet expectation.
	seedShard := func(t *testing.T, idx *Index, admitted, replaying bool) {
		t.Helper()
		shard := NewMockShardLike(t)
		if admitted && replaying {
			shard.EXPECT().preventShutdown().Return(func() {}, nil)
			shard.EXPECT().DeleteObject(mock.Anything, replay[0].ID, mock.Anything).Return(nil)
		}
		idx.shards.Store("t1", shard)
	}

	tests := []struct {
		name    string
		state   api.NamespaceState
		wantErr error
	}{
		{name: "active is admitted", state: api.NamespaceStateActive},
		{name: "suspended is admitted", state: api.NamespaceStateSuspended},
		{name: "resuming is admitted", state: api.NamespaceStateResuming},
		{name: "deleting is refused", state: api.NamespaceStateDeleting, wantErr: namespaces.ErrNamespaceDeleting},
	}

	for _, tc := range tests {
		t.Run("at the target load, "+tc.name, func(t *testing.T) {
			_, idx := dbForReopen(t, class, existerWithState(t, tc.state))
			seedShard(t, idx, tc.wantErr == nil, false)

			err := idx.LoadLocalShardForReplication(ctx, "t1")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})

		t.Run("at the change-log replay, "+tc.name, func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, tc.state))
			seedShard(t, idx, tc.wantErr == nil, true)

			err := idx.OverwriteObjectsFromChangeLog(ctx, "t1", replay)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})

		t.Run("at the movement's replica-add apply, "+tc.name, func(t *testing.T) {
			db, idx := dbForReopen(t, class, existerWithState(t, tc.state))
			seedShard(t, idx, tc.wantErr == nil, false)

			err := NewMigrator(db, idx.logger, "node1").LoadShardForReplication(ctx, class, "t1")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}

	for _, tc := range namespaceLookupRefusals() {
		t.Run(tc.name+" is refused at the target load", func(t *testing.T) {
			_, idx := dbForReopen(t, class, tc.exister(t))
			seedShard(t, idx, false, false)

			require.ErrorIs(t, idx.LoadLocalShardForReplication(ctx, "t1"), tc.wantErr)
		})

		t.Run(tc.name+" is refused at the change-log replay", func(t *testing.T) {
			idx := indexForGuardTest(t, class, tc.exister(t))
			seedShard(t, idx, false, false)

			require.ErrorIs(t, idx.OverwriteObjectsFromChangeLog(ctx, "t1", replay), tc.wantErr)
		})

		t.Run(tc.name+" is refused at the movement's replica-add apply", func(t *testing.T) {
			db, idx := dbForReopen(t, class, tc.exister(t))
			seedShard(t, idx, false, false)

			err := NewMigrator(db, idx.logger, "node1").LoadShardForReplication(ctx, class, "t1")
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	t.Run("an unqualified class name admits the replay", func(t *testing.T) {
		idx := indexForGuardTest(t, "Product", nil)
		seedShard(t, idx, true, true)

		require.NoError(t, idx.OverwriteObjectsFromChangeLog(ctx, "t1", replay))
	})

	// An empty replay writes nothing, so it returns before the namespace is even
	// consulted — a refusing state must not turn that no-op into an error.
	t.Run("an empty replay is a no-op in a refusing namespace", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateDeleting))
		seedShard(t, idx, false, false)

		require.NoError(t, idx.OverwriteObjectsFromChangeLog(ctx, "t1", nil))
	})

	// The exemption is per entry point, not per namespace: the same suspended
	// shard a movement may load and replay onto is still refused to a request.
	t.Run("the same suspended shard is refused to a request", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		seedShard(t, idx, true, true)

		require.NoError(t, idx.OverwriteObjectsFromChangeLog(ctx, "t1", replay))

		_, _, err := idx.getOrInitShard(ctx, "t1")
		require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
	})
}

// dbForSkipTest builds the DB an entry point resolves through, with shard t1
// seeded resident and its alloc checker failing every reservation, plus a hook on
// the index's log.
func dbForSkipTest(t *testing.T, className string, e namespaces.Exister) (*DB, *Index, *logrustest.Hook) {
	t.Helper()

	db, idx := dbForReopen(t, className, e)
	logger, hook := logrustest.NewNullLogger()
	idx.logger = logger
	idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})
	return db, idx, hook
}

// skipLoad runs an entry point against dbClass's index, naming callClass in the
// call itself so a case can point it at an index that is not there.
type skipLoad func(t *testing.T, dbClass, callClass string, e namespaces.Exister) (*logrustest.Hook, error)

// namespaceLoadState is one namespace state and whether an entry point deciding
// on it enters the shard load.
type namespaceLoadState struct {
	name     string
	state    api.NamespaceState
	wantLoad bool
}

// shardsShouldBeOpenStates is the state space of the check that decides whether
// a load is entered at all: two states open shards, three open none. Every
// entry point deciding on that check owes the same five answers.
func shardsShouldBeOpenStates() []namespaceLoadState {
	return []namespaceLoadState{
		{name: "an active namespace loads", state: api.NamespaceStateActive, wantLoad: true},
		{name: "a resuming namespace loads", state: api.NamespaceStateResuming, wantLoad: true},
		{name: "a suspended namespace loads nothing", state: api.NamespaceStateSuspended},
		{name: "a deleting namespace loads nothing", state: api.NamespaceStateDeleting},
		{name: "an unknown state loads nothing", state: api.NamespaceState("gone")},
	}
}

// appliedChangeStates is the same state space for an apply whose schema half has
// already committed, which differs from the check above on suspended: such an
// apply opens the shard it recorded rather than leaving the schema naming one
// this node does not hold.
func appliedChangeStates() []namespaceLoadState {
	return []namespaceLoadState{
		{name: "an active namespace loads", state: api.NamespaceStateActive, wantLoad: true},
		{name: "a resuming namespace loads", state: api.NamespaceStateResuming, wantLoad: true},
		{name: "a suspended namespace loads", state: api.NamespaceStateSuspended, wantLoad: true},
		{name: "a deleting namespace loads nothing", state: api.NamespaceStateDeleting},
		{name: "an unknown state loads nothing", state: api.NamespaceState("gone")},
	}
}

// firstSkippedState returns the state the given table expects to skip the load,
// so the log assertion below drives a skip whichever table it is handed.
func firstSkippedState(t *testing.T, states []namespaceLoadState) api.NamespaceState {
	t.Helper()

	for _, tc := range states {
		if !tc.wantLoad {
			return tc.state
		}
	}
	t.Fatal("no state in the table skips the load")
	return ""
}

// assertSkipsWhenNamespaceClosed drives the checks every entry point owes whose
// closed-namespace refusal yields nil: it loads in the states the given table
// admits, yields nil in the rest and says so in the log, and still errors on a
// state it cannot read or an index it cannot find.
//
// The seeded shard's alloc checker fails every reservation, so
// errInjectedMemoryPressure is what says the load was entered and nil is what says
// it was skipped.
func assertSkipsWhenNamespaceClosed(t *testing.T, class string, states []namespaceLoadState, load skipLoad) {
	t.Helper()

	for _, tc := range states {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, class, class, existerWithState(t, tc.state))
			if tc.wantLoad {
				require.ErrorIs(t, err, errInjectedMemoryPressure)
				return
			}
			require.NoError(t, err)
		})
	}

	t.Run("an unqualified class name loads", func(t *testing.T) {
		_, err := load(t, "Product", "Product", nil)
		require.ErrorIs(t, err, errInjectedMemoryPressure)
	})

	// Nothing else records that the schema change landed without a shard.
	t.Run("a skipped load is logged", func(t *testing.T) {
		hook, err := load(t, class, class, existerWithState(t, firstSkippedState(t, states)))
		require.NoError(t, err)

		entry := hook.LastEntry()
		require.NotNil(t, entry, "a skipped load must be logged")
		assert.Equal(t, logrus.InfoLevel, entry.Level)
		assert.Contains(t, entry.Message, errShardNamespaceClosed.Error())
	})

	// An unreadable state is not a namespace keeping its shards closed, so the skip
	// must not swallow it.
	for _, tc := range namespaceLookupRefusals() {
		t.Run(tc.name+" is an error, not a skipped load", func(t *testing.T) {
			_, err := load(t, class, class, tc.exister(t))
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	t.Run("a missing index is an error, not a silent success", func(t *testing.T) {
		_, err := load(t, class, "alpha:Other", existerWithState(t, api.NamespaceStateActive))
		require.Error(t, err)
	})
}

// The apply that adds a replica to a shard also runs without a movement behind
// it — node removal replacing a lost replica, and the scale plan's empty-source
// arm. Its schema half has already committed, so a suspended namespace opens the
// shard rather than leaving the sharding state naming a replica this node does
// not hold.
func TestReplicaAddWithoutMovement(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	assertSkipsWhenNamespaceClosed(t, class, appliedChangeStates(),
		func(t *testing.T, dbClass, callClass string, e namespaces.Exister) (*logrustest.Hook, error) {
			t.Helper()
			db, idx, hook := dbForSkipTest(t, dbClass, e)
			return hook, NewMigrator(db, idx.logger, "node1").LoadShardForReplicaAdd(ctx, callClass, "t1")
		})

	// The two admissions agree everywhere but here, so deleting is the only state
	// that goes red if the plain add is ever routed through the movement exemption.
	t.Run("a deleting namespace parts the plain add from a movement", func(t *testing.T) {
		db, idx, _ := dbForSkipTest(t, class, existerWithState(t, api.NamespaceStateDeleting))
		migrator := NewMigrator(db, idx.logger, "node1")

		require.NoError(t, migrator.LoadShardForReplicaAdd(ctx, class, "t1"))
		require.ErrorIs(t, migrator.LoadShardForReplication(ctx, class, "t1"), namespaces.ErrNamespaceDeleting)
	})
}

// The apply that records a finished offload or onload reaches the same shard load
// a tenant activation does, and a suspended namespace has to let it through. An
// unfreeze dropped the tenant's shard when it froze and has just downloaded its
// files back, so a skip there leaves the schema recording the tenant HOT with no
// shard behind it. The next request materializes one, so what this closes is the
// window in between rather than a permanently unreadable tenant.
//
// A namespace being deleted still skips, and that skip stays nil rather than
// failing the apply.
//
// HOT is the status both an unfreeze to HOT and an aborted freeze of a HOT tenant
// carry, which is why one status covers the two.
func TestTenantProcessShardLoad(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	reportHot := func(t *testing.T, className string, e namespaces.Exister, tenants ...string) error {
		t.Helper()
		db, idx := dbForReopen(t, className, e)
		updates := make([]*schemaUC.UpdateTenantPayload, len(tenants))
		for i, name := range tenants {
			idx.shards.Store(name, &LazyLoadShard{memMonitor: failingAllocChecker{}})
			updates[i] = &schemaUC.UpdateTenantPayload{Name: name, Status: models.TenantActivityStatusHOT}
		}
		return NewMigrator(db, idx.logger, "node1").
			UpdateTenantsForProcess(ctx, &models.Class{Class: className}, updates)
	}

	assertSkipsWhenNamespaceClosed(t, class, appliedChangeStates(),
		func(t *testing.T, dbClass, callClass string, e namespaces.Exister) (*logrustest.Hook, error) {
			t.Helper()
			db, idx, hook := dbForSkipTest(t, dbClass, e)
			return hook, NewMigrator(db, idx.logger, "node1").UpdateTenantsForProcess(ctx,
				&models.Class{Class: callClass},
				[]*schemaUC.UpdateTenantPayload{{Name: "t1", Status: models.TenantActivityStatusHOT}})
		})

	t.Run("no tenants reported is not an error", func(t *testing.T) {
		require.NoError(t, reportHot(t, class, existerWithState(t, api.NamespaceStateSuspended)))
	})

	batches := []struct {
		name    string
		state   api.NamespaceState
		wantErr error
	}{
		{
			name: "every tenant of a batch is loaded while suspended", state: api.NamespaceStateSuspended,
			wantErr: errInjectedMemoryPressure,
		},
		{name: "every tenant of a batch is skipped while deleting", state: api.NamespaceStateDeleting},
	}

	for _, tc := range batches {
		t.Run(tc.name, func(t *testing.T) {
			err := reportHot(t, class, existerWithState(t, tc.state), "t1", "t2", "t3")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}

	// A skipped load must not stop the rest of the batch: the COLD arm takes no
	// namespace check, so its shutdown has to run alongside the skip.
	t.Run("a skipped load does not abandon the other tenants", func(t *testing.T) {
		db, idx := dbForReopen(t, class, existerWithState(t, api.NamespaceStateDeleting))
		idx.shards.Store("hot", &LazyLoadShard{memMonitor: failingAllocChecker{}})
		cold := NewMockShardLike(t)
		cold.EXPECT().Shutdown(mock.Anything).Return(nil)
		idx.shards.Store("cold", cold)

		require.NoError(t, NewMigrator(db, idx.logger, "node1").UpdateTenantsForProcess(ctx,
			&models.Class{Class: class}, []*schemaUC.UpdateTenantPayload{
				{Name: "hot", Status: models.TenantActivityStatusHOT},
				{Name: "cold", Status: models.TenantActivityStatusCOLD},
			}))
	})

	// The split that goes red if the report is ever routed back through the
	// request path: the report opens the shard the same suspended namespace
	// refuses to a tenant activation.
	t.Run("a tenant activation is still refused on the same suspended shard", func(t *testing.T) {
		db, idx, _ := dbForSkipTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		updates := []*schemaUC.UpdateTenantPayload{{Name: "t1", Status: models.TenantActivityStatusHOT}}
		migrator := NewMigrator(db, idx.logger, "node1")

		require.ErrorIs(t, migrator.UpdateTenantsForProcess(ctx, &models.Class{Class: class}, updates),
			errInjectedMemoryPressure)
		require.ErrorIs(t, migrator.UpdateTenants(ctx, &models.Class{Class: class}, updates, true),
			namespaces.ErrNamespaceSuspended)
	})

	// The shard an unfreeze reports on is absent, not resident: freezing the
	// tenant dropped it, and nothing but this load registers it again.
	t.Run("an unfreeze report registers the shard it downloaded", func(t *testing.T) {
		idx, err := newIndexForNamespaceTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		require.NoError(t, err)

		require.NoError(t, idx.LoadLocalShardForTenantProcess(ctx, "t1"))
		require.NotNil(t, idx.shards.Load("t1"), "the reported tenant must end up with a shard")
	})
}

// A movement begins by touching its source shard, and both ways in take the
// request-path check — neither is exempt. That is what keeps a movement
// registered against a suspended namespace from starting, and it is the reason
// the target-side exemption needs no check of its own for whether a movement is
// under way: only one that began while the namespace was active can reach it.
func TestMovementCannotStartWhileSuspended(t *testing.T) {
	const class = "alpha:Product"
	const opID = "7"
	ctx := context.Background()

	entryPoints := []struct {
		name string
		call func(*Index) error
		// admit sets the calls the entry point makes once the namespace lets it
		// through, so the allow rows fail if it stops reaching the shard.
		admit func(*MockShardLike)
	}{
		{
			name: "starting change capture",
			call: func(idx *Index) error { return idx.IncomingStartChangeCapture(ctx, "t1", opID) },
			admit: func(s *MockShardLike) {
				s.EXPECT().ActivateChangeLog(mock.Anything, opID).Return(nil, nil)
			},
		},
		{
			name: "creating the replica snapshot",
			call: func(idx *Index) error {
				_, err := idx.IncomingCreateReplicaSnapshot(ctx, "t1", opID)
				return err
			},
			admit: func(s *MockShardLike) {
				s.EXPECT().CreateReplicaSnapshot(mock.Anything, mock.Anything).Return(nil, nil)
			},
		},
	}

	refused := []struct {
		name    string
		state   api.NamespaceState
		wantErr error
	}{
		{name: "suspended", state: api.NamespaceStateSuspended, wantErr: namespaces.ErrNamespaceSuspended},
		{name: "resuming", state: api.NamespaceStateResuming, wantErr: namespaces.ErrNamespaceResuming},
		{name: "deleting", state: api.NamespaceStateDeleting, wantErr: namespaces.ErrNamespaceDeleting},
	}

	for _, ep := range entryPoints {
		for _, tc := range refused {
			t.Run(ep.name+" is refused while "+tc.name, func(t *testing.T) {
				idx := indexForGuardTest(t, class, existerWithState(t, tc.state))
				// Resident, so a refusal cannot be mistaken for an absent shard.
				idx.shards.Store("t1", NewMockShardLike(t))

				require.ErrorIs(t, ep.call(idx), tc.wantErr)
			})
		}

		// Without this, an entry point that refused every state would still pass
		// every row above.
		t.Run(ep.name+" is admitted while active", func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateActive))
			shard := NewMockShardLike(t)
			shard.EXPECT().preventShutdown().Return(func() {}, nil)
			ep.admit(shard)
			idx.shards.Store("t1", shard)

			require.NoError(t, ep.call(idx))
		})
	}
}

// getOptInitLocalShard is the read/write request path, which has two load points
// the load path does not cover: the ensureInit creation, and preventShutdown
// loading a resident lazy shard even when ensureInit is false. The guard sits
// above both.
//
// Each case is discriminating in its own way. Without a resident shard and with
// ensureInit false the function otherwise returns no shard and no error, so a
// refusal there is red if the guard moves into the ensureInit branch. With a
// resident zero-value LazyLoadShard, reaching preventShutdown panics on Load, so
// a namespace error rather than a panic is what proves the guard ran first.
func TestGuardRequestPath(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	refused := []struct {
		name    string
		state   api.NamespaceState
		wantErr error
	}{
		{name: "suspended", state: api.NamespaceStateSuspended, wantErr: namespaces.ErrNamespaceSuspended},
		{name: "deleting", state: api.NamespaceStateDeleting, wantErr: namespaces.ErrNamespaceDeleting},
		{name: "resuming", state: api.NamespaceStateResuming, wantErr: namespaces.ErrNamespaceResuming},
	}

	for _, tc := range refused {
		t.Run("a read refuses "+tc.name+" with no resident shard", func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, tc.state))

			shard, release, err := idx.GetShard(ctx, "t1")
			require.ErrorIs(t, err, tc.wantErr)
			assert.Nil(t, shard)
			require.NotNil(t, release, "a nil release panics in a caller that defers it")
		})

		t.Run("a read refuses "+tc.name+" holding a resident lazy shard", func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, tc.state))
			lazy := &LazyLoadShard{}
			idx.shards.Store("t1", lazy)

			shard, release, err := idx.GetShard(ctx, "t1")
			require.ErrorIs(t, err, tc.wantErr)
			assert.Nil(t, shard)
			require.NotNil(t, release)
			assert.False(t, lazy.loaded, "the resident shard must not have been loaded")
		})

		t.Run("a write refuses "+tc.name, func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, tc.state))

			_, _, err := idx.getOrInitShard(ctx, "t1")
			require.ErrorIs(t, err, tc.wantErr)
		})
	}

	t.Run("a lookup miss refuses", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		idx := indexForGuardTest(t, class, e)
		idx.shards.Store("t1", &LazyLoadShard{})

		_, _, err := idx.GetShard(ctx, "t1")
		require.ErrorIs(t, err, errNamespaceUnknownLocally)
	})

	// The allow side. An absent shard is the one case that reaches a clean return
	// without a collaborator, so it is what the admitted states assert on: a guard
	// refusing every read would pass every refusal above.
	t.Run("an active namespace admits the read", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateActive))

		shard, _, err := idx.GetShard(ctx, "t1")
		require.NoError(t, err)
		assert.Nil(t, shard, "an absent shard is reported absent, not created")
	})

	t.Run("an unqualified class name admits the read", func(t *testing.T) {
		idx := indexForGuardTest(t, "Product", nil)

		shard, _, err := idx.GetShard(ctx, "t1")
		require.NoError(t, err)
		assert.Nil(t, shard)
	})
}

// The cleanup entry points act on a shard the caller already opened, so a
// namespace that is not active must not refuse them: that would leave a
// change-capture log registered, or a shard halted with nothing to resume it.
func TestCleanupPathsSkipTheGuard(t *testing.T) {
	const (
		class     = "alpha:Product"
		shardName = "t1"
		opID      = "op-1"
	)
	ctx := context.Background()

	// An exister with no expectations fails the test on any lookup, which is the
	// claim: these paths decide nothing from the namespace, whatever its state.

	t.Run("change capture stops without a namespace lookup", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
		shard := NewMockShardLike(t)
		shard.On("preventShutdown").Return(func() {}, nil).Once()
		shard.On("StopChangeCapture", mock.Anything, opID).Return(nil).Once()
		idx.shards.Store(shardName, shard)

		require.NoError(t, idx.IncomingStopChangeCapture(ctx, shardName, opID))
	})

	t.Run("the transfer inactivity timer resets without a namespace lookup", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
		shard := NewMockShardLike(t)
		shard.On("MayResetTransferInactivityTimer").Once()
		idx.shards.Store(shardName, shard)
		idx.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName})

		idx.mayResetReplicaSnapshotInactivity(opID)
	})

	t.Run("the snapshot release resumes the shard without a namespace lookup", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
		shard := NewMockShardLike(t)
		shard.On("preventShutdown").Return(func() {}, nil).Once()
		shard.On("resumeMaintenanceCycles", mock.Anything).Return(nil).Once()
		idx.shards.Store(shardName, shard)
		idx.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName})

		require.NoError(t, idx.releaseReplicaSnapshot(ctx, opID, nil))
	})

	// The same shard a teardown may reach is still refused to a request.
	t.Run("a request for the same shard is still refused", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		shard := NewMockShardLike(t)
		shard.On("preventShutdown").Return(func() {}, nil).Once()
		shard.On("StopChangeCapture", mock.Anything, opID).Return(nil).Once()
		idx.shards.Store(shardName, shard)

		require.NoError(t, idx.IncomingStopChangeCapture(ctx, shardName, opID))

		_, _, err := idx.getOrInitShard(ctx, shardName)
		require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
	})

	// Loading a zero-value LazyLoadShard panics, so passing proves nothing loaded it.
	t.Run("an unloaded shard is left unloaded", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
		lazy := &LazyLoadShard{}
		idx.shards.Store(shardName, lazy)
		idx.recordReplicaSnapshot(opID, replicaSnapshotState{shardName: shardName})

		require.NoError(t, idx.IncomingStopChangeCapture(ctx, shardName, opID))
		idx.mayResetReplicaSnapshotInactivity(opID)
		require.NoError(t, idx.releaseReplicaSnapshot(ctx, opID, nil))
		assert.False(t, lazy.isLoaded(), "cleanup must not load the shard")
	})

	t.Run("an absent shard holds no log to stop", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))

		require.NoError(t, idx.IncomingStopChangeCapture(ctx, shardName, opID))
	})

	// A failed teardown must not report success.
	t.Run("a failing shard surfaces its error", func(t *testing.T) {
		idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
		shard := NewMockShardLike(t)
		shard.On("preventShutdown").Return(func() {}, nil).Once()
		shard.On("StopChangeCapture", mock.Anything, opID).
			Return(errors.New("deactivate failed")).Once()
		idx.shards.Store(shardName, shard)

		require.ErrorContains(t, idx.IncomingStopChangeCapture(ctx, shardName, opID), "deactivate failed")
	})
}

// The change-log drain reads the loaded shard directly, so a movement already
// capturing changes finishes even while the namespace is suspended. Only opening
// a capture takes the request-path check. Guarding any of the drain endpoints
// would cancel an in-flight movement instead.
func TestChangeLogDrainSkipsTheGuard(t *testing.T) {
	const (
		class     = "alpha:Product"
		shardName = "t1"
		opID      = "op-1"
	)
	ctx := context.Background()

	drains := []struct {
		name string
		call func(t *testing.T, idx *Index, shard *MockShardLike)
	}{
		{
			name: "get change log",
			call: func(t *testing.T, idx *Index, shard *MockShardLike) {
				log, err := changelog.Open(filepath.Join(t.TempDir(), opID), idx.logger)
				require.NoError(t, err)
				shard.On("GetChangeLog", mock.Anything, opID).Return(log, true).Once()

				tailer, err := idx.IncomingGetChangeLog(ctx, shardName, opID, 1)
				require.NoError(t, err)
				require.NoError(t, tailer.Close())
			},
		},
		{
			name: "snapshot change-log LSN",
			call: func(t *testing.T, idx *Index, shard *MockShardLike) {
				shard.On("SnapshotChangeLogLSN", mock.Anything, opID).Return(uint64(7), nil).Once()

				lsn, err := idx.IncomingSnapshotChangeLogLSN(ctx, shardName, opID)
				require.NoError(t, err)
				assert.Equal(t, uint64(7), lsn)
			},
		},
		{
			name: "finalize change log",
			call: func(t *testing.T, idx *Index, shard *MockShardLike) {
				shard.On("FinalizeChangeLog", mock.Anything, opID).Return(uint64(9), nil).Once()

				lsn, err := idx.IncomingFinalizeChangeLog(ctx, shardName, opID)
				require.NoError(t, err)
				assert.Equal(t, uint64(9), lsn)
			},
		},
	}

	for _, tc := range drains {
		// An exister with no expectations fails the test on any lookup.
		t.Run(tc.name+" takes no namespace lookup", func(t *testing.T) {
			idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
			shard := NewMockShardLike(t)
			shard.On("preventShutdown").Return(func() {}, nil).Once()
			idx.shards.Store(shardName, shard)

			tc.call(t, idx, shard)
		})

		t.Run(tc.name+" is served while the namespace is suspended", func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
			shard := NewMockShardLike(t)
			shard.On("preventShutdown").Return(func() {}, nil).Once()
			idx.shards.Store(shardName, shard)

			tc.call(t, idx, shard)
		})
	}

	// The boundary the rows above are read against.
	t.Run("opening a capture is refused while suspended", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		idx.shards.Store(shardName, NewMockShardLike(t))

		err := idx.IncomingStartChangeCapture(ctx, shardName, opID)
		require.ErrorIs(t, err, namespaces.ErrNamespaceSuspended)
	})
}

// replicaResponseErrors reads the errors out of the response
// [Index.CommitReplication] and [Index.AbortReplication] return as any.
func replicaResponseErrors(t *testing.T, resp any) []replicaerrors.Error {
	t.Helper()
	simple, ok := resp.(replica.SimpleResponse)
	require.True(t, ok, "expected a replica.SimpleResponse, got %T", resp)
	return simple.Errors
}

// A replicated write prepares on one call and commits or aborts on another.
// Only the prepare asks the namespace whether the write may proceed. Refusing
// the second call leaves the prepared task in the shard's map with nothing to
// remove it, which strands the write and blocks every later seal of that shard.
func TestTwoPhaseCommitSkipsTheGuard(t *testing.T) {
	const (
		class     = "alpha:Product"
		shardName = "t1"
		requestID = "req-1"
	)
	ctx := context.Background()

	// A commit needs the task it executes, so it reports an unloaded shard as a
	// missing request. An abort has nothing to remove and is already done.
	verbs := []struct {
		name              string
		nilOnMissingShard bool
		expect            func(shard *MockShardLike)
		call              func(idx *Index) any
	}{
		{
			name:              "commit",
			nilOnMissingShard: true,
			expect: func(shard *MockShardLike) {
				shard.On("commitReplication", mock.Anything, requestID).Return(replica.SimpleResponse{}).Once()
			},
			call: func(idx *Index) any { return idx.CommitReplication(ctx, shardName, requestID) },
		},
		{
			name: "abort",
			expect: func(shard *MockShardLike) {
				shard.On("abortReplication", mock.Anything, requestID).Return(replica.SimpleResponse{}).Once()
			},
			call: func(idx *Index) any { return idx.AbortReplication(ctx, shardName, requestID) },
		},
	}

	for _, tc := range verbs {
		// An exister with no expectations fails the test on any lookup.
		t.Run(tc.name+" takes no namespace lookup", func(t *testing.T) {
			idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
			shard := NewMockShardLike(t)
			shard.On("preventShutdown").Return(func() {}, nil).Once()
			tc.expect(shard)
			idx.shards.Store(shardName, shard)

			assert.Empty(t, replicaResponseErrors(t, tc.call(idx)))
		})

		t.Run(tc.name+" is served while the namespace is suspended", func(t *testing.T) {
			idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
			shard := NewMockShardLike(t)
			shard.On("preventShutdown").Return(func() {}, nil).Once()
			tc.expect(shard)
			idx.shards.Store(shardName, shard)

			assert.Empty(t, replicaResponseErrors(t, tc.call(idx)))
		})

		// Loading a zero-value LazyLoadShard panics, so passing proves nothing
		// materialized a shard that can no longer hold the task.
		t.Run(tc.name+" on an unloaded shard leaves it unloaded", func(t *testing.T) {
			idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
			lazy := &LazyLoadShard{}
			idx.shards.Store(shardName, lazy)

			resp := tc.call(idx)
			if tc.nilOnMissingShard {
				// Both transports read a nil response as "request not found",
				// which no typed commit response can decode as a success.
				assert.Nil(t, resp)
			} else {
				assert.Empty(t, replicaResponseErrors(t, resp))
			}
			assert.False(t, lazy.isLoaded())
		})

		// A missing shard is what makes an abort already done. A failed lookup
		// is not, so a closed index still reports failure.
		t.Run(tc.name+" reports a closed index", func(t *testing.T) {
			idx := indexForGuardTest(t, class, namespaces.NewMockExister(t))
			idx.closed = true

			errs := replicaResponseErrors(t, tc.call(idx))
			require.Len(t, errs, 1)
			assert.Equal(t, replicaerrors.StatusShardNotFound, errs[0].Code)
			assert.ErrorIs(t, errs[0].Err, errAlreadyShutdown)
		})
	}

	// The boundary the rows above are read against.
	t.Run("preparing a write is refused while suspended", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateSuspended))
		idx.shards.Store(shardName, NewMockShardLike(t))

		resp := idx.ReplicateObject(ctx, shardName, requestID, &storobj.Object{}, 0)
		require.Len(t, resp.Errors, 1)
		require.ErrorIs(t, resp.Errors[0].Err, namespaces.ErrNamespaceSuspended)
	})
}

// indexForBootTest builds the minimum Index initAndStoreShards needs. Lazy
// loading keeps shard construction off disk, so what the test observes is which
// shards were registered, not what they contain.
func indexForBootTest(t *testing.T, className string, e namespaces.Exister, reader schemaUC.SchemaReader) (*Index, *logrustest.Hook) {
	t.Helper()

	logger, hook := logrustest.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	idx := &Index{
		Config: IndexConfig{
			ClassName:            schema.ClassName(className),
			RootPath:             t.TempDir(),
			EnableLazyLoadShards: true,
		},
		namespace:         namespacing.NamespaceFromQualified(className),
		namespacesExister: e,
		logger:            logger,
		schemaReader:      reader,
		shardCreateLocks:  esync.NewKeyRWLocker(),
		closingCtx:        ctx,
	}
	idx.closeRequestedCtx, idx.signalCloseRequested = context.WithCancelCause(context.Background())
	return idx, hook
}

func registeredShards(t *testing.T, idx *Index) []string {
	t.Helper()

	var names []string
	require.NoError(t, idx.ForEachShard(func(name string, _ ShardLike) error {
		names = append(names, name)
		return nil
	}))
	sort.Strings(names)
	return names
}

// initAndStoreShards runs at boot: the constructor registers a class's local
// shards before any request can reach the index. It filters on
// ShardsShouldBeOpen rather than the request-path check, so a resuming namespace
// keeps its HOT shards — otherwise nothing reopens and a resume could never
// finish.
func TestGuardBoot(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	// empty1 pins that a tenant with no status counts as HOT on boot.
	mixed := map[string]sharding.Physical{
		"hot1": hotPhysical("hot1"), "empty1": localPhysical("empty1"), "cold1": coldPhysical("cold1"),
	}

	tests := []struct {
		name       string
		className  string
		state      api.NamespaceState
		namespaced bool
		want       []string
	}{
		{
			name: "a suspended class registers nothing", className: class,
			state: api.NamespaceStateSuspended, namespaced: true,
		},
		{
			name: "a deleting class registers nothing", className: class,
			state: api.NamespaceStateDeleting, namespaced: true,
		},
		{
			// Red if the filter is the request-path check, which rejects resuming.
			name: "a resuming class keeps its HOT shards", className: class,
			state: api.NamespaceStateResuming, namespaced: true,
			want: []string{"empty1", "hot1"},
		},
		{
			name: "an active class keeps its HOT shards", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			want: []string{"empty1", "hot1"},
		},
		{
			name: "an unqualified class keeps its HOT shards", className: "Product",
			want: []string{"empty1", "hot1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e namespaces.Exister
			if tc.namespaced {
				e = existerWithState(t, tc.state)
			}
			idx, _ := indexForBootTest(t, tc.className, e,
				readerForShards(t, tc.className, mixed))

			require.NoError(t, idx.initAndStoreShards(ctx, &models.Class{Class: tc.className}, nil))
			assert.Equal(t, tc.want, registeredShards(t, idx))

			if len(tc.want) == 0 {
				// An index with nothing to load is ready. Left false, it would stop
				// the node-wide object count for every other index too.
				assert.True(t, idx.allShardsReady.Load(),
					"a class that registers no shards must still report ready")
			}
		})
	}

	// A state that cannot be read is not a namespace keeping its shards closed:
	// the class may hold data on disk, so boot must refuse rather than register
	// nothing and report the class ready.
	refusals := []struct {
		name    string
		exister func(*testing.T) namespaces.Exister
		wantErr error
	}{
		{
			name:    "a missing lookup",
			exister: func(*testing.T) namespaces.Exister { return nil },
			wantErr: errNoNamespaceLookup,
		},
		{
			name: "a lookup miss",
			exister: func(t *testing.T) namespaces.Exister {
				e := namespaces.NewMockExister(t)
				e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
				return e
			},
			wantErr: errNamespaceUnknownLocally,
		},
	}

	for _, tc := range refusals {
		t.Run(tc.name+" returns an error instead of reporting the class ready", func(t *testing.T) {
			idx, hook := indexForBootTest(t, class, tc.exister(t), readerForShards(t, class, mixed))

			err := idx.initAndStoreShards(ctx, &models.Class{Class: class}, nil)
			require.ErrorIs(t, err, tc.wantErr)
			assert.Empty(t, registeredShards(t, idx))
			assert.False(t, idx.allShardsReady.Load(),
				"a class whose shards were never enumerated must not report ready")

			entry := hook.LastEntry()
			require.NotNil(t, entry, "a refusal must be logged")
			assert.Equal(t, logrus.ErrorLevel, entry.Level)
		})
	}

	// Registering no Read expectation asserts the sharding state is never read:
	// when nothing may be open, boot must not walk every tenant to learn that.
	t.Run("a suspended class is decided without reading the sharding state", func(t *testing.T) {
		idx, _ := indexForBootTest(t, class, existerWithState(t, api.NamespaceStateSuspended),
			schemaUC.NewMockSchemaReader(t))

		require.NoError(t, idx.initAndStoreShards(ctx, &models.Class{Class: class}, nil))
		assert.Empty(t, registeredShards(t, idx))
	})
}

// initAndStoreShards hands its shard names to a loop that loads them one per
// tick, so the state read at boot is stale by the time a later shard comes up.
// Each load re-reads it, and refuses by returning nil so the loop carries on to
// the shards behind it.
//
// The seeded shard's alloc checker fails every reservation, so an error reaching
// the caller is what says LazyLoadShard.Load was entered.
func TestGuardBackgroundLoad(t *testing.T) {
	const class = "alpha:Product"

	tests := []struct {
		name       string
		className  string
		state      api.NamespaceState
		namespaced bool
		wantLoad   bool
	}{
		{
			name: "an active namespace loads", className: class,
			state: api.NamespaceStateActive, namespaced: true, wantLoad: true,
		},
		{
			// Red if the filter is the request-path check, which rejects resuming.
			name: "a resuming namespace loads", className: class,
			state: api.NamespaceStateResuming, namespaced: true, wantLoad: true,
		},
		{
			name: "a namespace suspended after boot loads nothing", className: class,
			state: api.NamespaceStateSuspended, namespaced: true,
		},
		{
			name: "a deleting namespace loads nothing", className: class,
			state: api.NamespaceStateDeleting, namespaced: true,
		},
		{
			name: "an unknown state loads nothing", className: class,
			state: api.NamespaceState("gone"), namespaced: true,
		},
		{
			name: "an unqualified class loads", className: "Product", wantLoad: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e namespaces.Exister
			if tc.namespaced {
				e = existerWithState(t, tc.state)
			}
			idx := indexForGuardTest(t, tc.className, e)
			idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})

			err := idx.loadLocalShardIfActive("t1")
			if tc.wantLoad {
				require.ErrorIs(t, err, errInjectedMemoryPressure)
			} else {
				require.NoError(t, err)
			}
		})
	}

	// Index.Shutdown sweeps i.shards without taking shardCreateLocks, and skips
	// a lazy shard that is not loaded. A load admitted after that sweep would
	// build a shard nothing is left to close.
	t.Run("a shut-down index loads nothing", func(t *testing.T) {
		idx := indexForGuardTest(t, class, existerWithState(t, api.NamespaceStateActive))
		idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})
		idx.closed = true

		require.NoError(t, idx.loadLocalShardIfActive("t1"))
	})

	// A state that cannot be read refuses the load like a closed namespace does,
	// and returns nil for the same reason: one unreadable read must not stop the
	// shards behind it from being tried.
	refusals := []struct {
		name    string
		exister func(*testing.T) namespaces.Exister
	}{
		{
			name:    "a missing lookup",
			exister: func(*testing.T) namespaces.Exister { return nil },
		},
		{
			name: "a lookup miss",
			exister: func(t *testing.T) namespaces.Exister {
				e := namespaces.NewMockExister(t)
				e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
				return e
			},
		},
	}

	for _, tc := range refusals {
		t.Run(tc.name+" loads nothing", func(t *testing.T) {
			idx := indexForGuardTest(t, class, tc.exister(t))
			idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})

			require.NoError(t, idx.loadLocalShardIfActive("t1"))
		})
	}

	// The one case that drives the loop rather than a single load, because the
	// state lookup count is what says the loop carried on: it reaches three only
	// by asking again for the second shard. The alloc checker turns any admitted
	// load into the error the loop stops on, so the absent log is the other half.
	t.Run("a suspend after boot stops the loop opening shards, not the loop", func(t *testing.T) {
		var lookups atomic.Int32
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").RunAndReturn(func(string) (api.Namespace, bool) {
			state := api.NamespaceStateSuspended
			if lookups.Add(1) == 1 {
				// The boot read, which has to admit or no loop starts at all.
				state = api.NamespaceStateActive
			}
			return api.Namespace{Name: "alpha", State: state}, true
		}).Maybe()

		shards := map[string]sharding.Physical{"t1": hotPhysical("t1"), "t2": hotPhysical("t2")}
		idx, hook := indexForBootTest(t, class, e, readerForShards(t, class, shards))
		idx.allocChecker = failingAllocChecker{}

		require.NoError(t, idx.initAndStoreShards(context.Background(), &models.Class{Class: class}, nil))

		require.Eventually(t, func() bool { return lookups.Load() >= 3 }, 10*time.Second, 20*time.Millisecond,
			"the loop stopped instead of asking again for the second shard")
		for _, entry := range hook.AllEntries() {
			assert.NotContains(t, entry.Message, "failed to load shard")
		}
		assert.Equal(t, []string{"t1", "t2"}, registeredShards(t, idx),
			"a refused shard stays registered, so a resumed namespace still has something to load")
	})
}

// Boot and the multi-tenant reload read an empty tenant status differently: boot
// normalizes it to HOT and loads, while the reload compares Status raw and
// unloads. Neither filter changes here, so both behaviours are pinned — a later
// unification of the two cannot happen silently.
func TestEmptyTenantStatusBootVsReload(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	t.Run("boot loads a tenant with no status", func(t *testing.T) {
		shards := map[string]sharding.Physical{"empty1": localPhysical("empty1")}
		idx, _ := indexForBootTest(t, class, existerWithState(t, api.NamespaceStateActive),
			readerForShards(t, class, shards))

		require.NoError(t, idx.initAndStoreShards(ctx, &models.Class{Class: class}, nil))
		assert.Equal(t, []string{"empty1"}, registeredShards(t, idx))
	})

	t.Run("the multi-tenant reload unloads a tenant with no status", func(t *testing.T) {
		idx, _ := indexForBootTest(t, class, existerWithState(t, api.NamespaceStateActive),
			schemaUC.NewMockSchemaReader(t))

		shard := NewMockShardLike(t)
		shard.EXPECT().Shutdown(mock.Anything).Return(nil)
		idx.shards.Store("empty1", shard)

		sg := schemaUC.NewMockSchemaGetter(t)
		sg.EXPECT().NodeName().Return("node1")
		m := &Migrator{db: &DB{schemaGetter: sg}}

		incoming := &sharding.State{Physical: map[string]sharding.Physical{"empty1": localPhysical("empty1")}}
		require.NoError(t, m.updateIndexTenantsStatus(ctx, idx, incoming))

		assert.Empty(t, registeredShards(t, idx), "the tenant must have been unloaded")
	})
}

// migratorForReload builds the Migrator UpdateIndex resolves through. Lazy
// loading keeps a shard the reload creates off disk, so what a case observes is
// which shards ended up registered.
func migratorForReload(t *testing.T, className string, e namespaces.Exister) (*Migrator, *Index) {
	t.Helper()

	db, idx := dbForReopen(t, className, e)
	idx.Config.EnableLazyLoadShards = true
	idx.metrics = &Metrics{}
	return NewMigrator(db, idx.logger, "node1"), idx
}

// reloadClass is the class a reload carries: one property, so a shard missing
// its bucket gives the property step something to do.
func reloadClass(className string) *models.Class {
	return &models.Class{Class: className, Properties: []*models.Property{{Name: "title"}}}
}

func reloadState(partitioned bool, shards map[string]sharding.Physical) *sharding.State {
	state := &sharding.State{PartitioningEnabled: partitioned, Physical: shards}
	state.SetLocalName("node1")
	return state
}

// seedShardMissingProperty registers a shard the reload finds open and without
// the class's property bucket: an empty store answers every bucket lookup with
// nil. The expectations are what say the property step ran at all.
func seedShardMissingProperty(t *testing.T, idx *Index, name string) {
	t.Helper()

	dir := t.TempDir()
	store, err := lsmkv.New(dir, dir, idx.logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(context.Background()) })

	shard := NewMockShardLike(t)
	shard.EXPECT().Store().Return(store)
	shard.EXPECT().initPropertyBuckets(mock.Anything, mock.Anything, false, mock.Anything)
	idx.shards.Store(name, shard)
}

// A reload matches the loaded tenants to the incoming sharding state, then
// drops the tenants that state no longer lists and adds the properties the
// class gained. A namespace that is not active must not turn that into an early
// return: a failed reload is never retried, since ReloadLocalDB's own caller
// drops its error, and ranging over a map makes what one skipped a different
// set per node and per run.
func TestGuardReload(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	// Active is the control the refusing states are read against: dropping the
	// tenants the schema no longer lists, unloading the ones turned COLD, and
	// adding the class's new property are owed whatever the state. Only which
	// shards end up open turns on it, so that is the one column per row.
	states := []struct {
		name             string
		className        string
		state            api.NamespaceState
		namespaced       bool
		wantSingleTenant []string
	}{
		{
			name: "an active namespace", className: class,
			state: api.NamespaceStateActive, namespaced: true,
			wantSingleTenant: []string{"s1", "s2", "s3"},
		},
		{
			// Not {"s1"}: a resuming namespace's shards do reopen.
			name: "a resuming namespace", className: class,
			state: api.NamespaceStateResuming, namespaced: true,
			wantSingleTenant: []string{"s1", "s2", "s3"},
		},
		{
			name: "a suspended namespace", className: class,
			state: api.NamespaceStateSuspended, namespaced: true,
			wantSingleTenant: []string{"s1"},
		},
		{
			name: "a deleting namespace", className: class,
			state: api.NamespaceStateDeleting, namespaced: true,
			wantSingleTenant: []string{"s1"},
		},
		{
			name: "an unknown state", className: class,
			state: api.NamespaceState("gone"), namespaced: true,
			wantSingleTenant: []string{"s1"},
		},
		{
			// Every class on a cluster running with namespaces off, so this is
			// the row that says the reload is unchanged there. The rows above
			// cannot show it: each one goes through the lookup.
			name: "an unqualified class", className: "Product",
			wantSingleTenant: []string{"s1", "s2", "s3"},
		},
	}

	for _, tc := range states {
		exister := func(t *testing.T) namespaces.Exister {
			t.Helper()
			if !tc.namespaced {
				return nil
			}
			return existerWithState(t, tc.state)
		}

		// open1 is HOT and already resident, the shape a movement's target shard
		// has while its namespace is suspended: the reload reaches it, and the
		// property step behind the abort is what must still see it. Two tenants
		// of each kind, because one leaves "the rest of the loop ran" resting on
		// the order the map happened to yield.
		t.Run(tc.name+" finishes the multi-tenant reload", func(t *testing.T) {
			m, idx := migratorForReload(t, tc.className, exister(t))
			seedShardMissingProperty(t, idx, "open1")

			for _, name := range []string{"cold1", "cold2"} {
				cold := NewMockShardLike(t)
				cold.EXPECT().Shutdown(mock.Anything).Return(nil)
				idx.shards.Store(name, cold)
			}

			// Absent from the incoming state, so the reload owns removing them.
			// Left behind, their directories and offloaded copies are orphaned.
			storeDroppableShard(t, idx, "gone1", nil)
			storeDroppableShard(t, idx, "gone2", nil)

			state := reloadState(true, map[string]sharding.Physical{
				"open1": hotPhysical("open1"),
				"cold1": coldPhysical("cold1"), "cold2": coldPhysical("cold2"),
			})

			require.NoError(t, m.UpdateIndex(ctx, reloadClass(tc.className), state))
			assert.Equal(t, []string{"open1"}, registeredShards(t, idx))
		})

		// s2 and s3 are listed for this node and not resident, so the reload
		// creates them. The property step sits behind that creation.
		t.Run(tc.name+" finishes the single-tenant reload", func(t *testing.T) {
			m, idx := migratorForReload(t, tc.className, exister(t))
			seedShardMissingProperty(t, idx, "s1")

			state := reloadState(false, map[string]sharding.Physical{
				"s1": localPhysical("s1"), "s2": localPhysical("s2"), "s3": localPhysical("s3"),
			})

			require.NoError(t, m.UpdateIndex(ctx, reloadClass(tc.className), state))
			assert.Equal(t, tc.wantSingleTenant, registeredShards(t, idx))
		})
	}

	// Which tenants a reload opens is decided by the check boot uses rather than
	// the request-path one: a resuming namespace's shards have to reopen for the
	// resume to finish.
	t.Run("which tenants the reload opens", func(t *testing.T) {
		for _, tc := range shardsShouldBeOpenStates() {
			t.Run(tc.name, func(t *testing.T) {
				m, idx := migratorForReload(t, class, existerWithState(t, tc.state))
				idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})

				state := reloadState(true, map[string]sharding.Physical{"t1": hotPhysical("t1")})

				err := m.UpdateIndex(ctx, reloadClass(class), state)
				if tc.wantLoad {
					require.ErrorIs(t, err, errInjectedMemoryPressure)
					return
				}
				require.NoError(t, err)
			})
		}
	})

	// A state that cannot be read is not a namespace keeping its shards closed:
	// the class may hold data on disk, so the reload must fail rather than carry
	// on as if it had decided.
	for _, tc := range namespaceLookupRefusals() {
		t.Run(tc.name+" fails the reload", func(t *testing.T) {
			m, idx := migratorForReload(t, class, tc.exister(t))
			idx.shards.Store("t1", NewMockShardLike(t))

			state := reloadState(true, map[string]sharding.Physical{"t1": hotPhysical("t1")})

			require.ErrorIs(t, m.UpdateIndex(ctx, reloadClass(class), state), tc.wantErr)
		})
	}
}

// A class whose shards no loading path will open must not count toward startup
// progress: nothing ever loads them, so the gauge would never complete.
func TestLocalShardsToLoad(t *testing.T) {
	const class = "alpha:Product"

	mixed := map[string]sharding.Physical{
		"hot1": hotPhysical("hot1"), "empty1": localPhysical("empty1"), "cold1": coldPhysical("cold1"),
	}

	tests := []struct {
		name       string
		className  string
		state      api.NamespaceState
		namespaced bool
		want       int64
	}{
		{name: "an active class counts its HOT shards", className: class, state: api.NamespaceStateActive, namespaced: true, want: 2},
		{name: "a suspended class counts none", className: class, state: api.NamespaceStateSuspended, namespaced: true},
		{name: "a deleting class counts none", className: class, state: api.NamespaceStateDeleting, namespaced: true},
		{
			// Not 0: a resuming namespace's shards do reopen, so they still count.
			name: "a resuming class counts its HOT shards", className: class,
			state: api.NamespaceStateResuming, namespaced: true, want: 2,
		},
		{name: "an unqualified class counts its HOT shards", className: "Product", want: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var e namespaces.Exister
			if tc.namespaced {
				e = existerWithState(t, tc.state)
			}
			db := dbForDesiredOpen(t, tc.className, e, mixed)

			assert.Equal(t, tc.want, db.localShardsToLoad(tc.className))
		})
	}

	t.Run("a lookup miss counts none and logs", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		logger, hook := logrustest.NewNullLogger()
		db := &DB{
			logger:            logger,
			schemaReader:      readerForShards(t, class, mixed),
			namespacesExister: e,
		}

		assert.Zero(t, db.localShardsToLoad(class))

		entry := hook.LastEntry()
		require.NotNil(t, entry, "a lookup miss must be logged")
		assert.Equal(t, logrus.ErrorLevel, entry.Level)
	})
}

// dbForReopen builds the DB shape ReopenShard resolves through: one index,
// reachable under the key GetIndex looks up, and the node name a tenant loop
// filters its local shards on.
func dbForReopen(t *testing.T, className string, e namespaces.Exister) (*DB, *Index) {
	t.Helper()

	idx := indexForGuardTest(t, className, e)
	sg := schemaUC.NewMockSchemaGetter(t)
	sg.EXPECT().ReadOnlyClass(className).Return(&models.Class{Class: className}).Maybe()
	sg.EXPECT().NodeName().Return("node1").Maybe()
	idx.getSchema = sg

	logger, _ := logrustest.NewNullLogger()
	return &DB{logger: logger, schemaGetter: sg, indices: map[string]*Index{idx.ID(): idx}}, idx
}

// ReopenShard is the entry point a resuming namespace's shards come back
// through. A resident non-lazy shard is already open, so the call returns nil
// once admitted — which is what makes admission the only variable here.
func TestReopenShard(t *testing.T) {
	const class = "alpha:Product"
	ctx := context.Background()

	tests := []struct {
		name    string
		state   api.NamespaceState
		wantErr error
	}{
		{name: "resuming is reopened", state: api.NamespaceStateResuming},
		{name: "active is reopened", state: api.NamespaceStateActive},
		{name: "suspended is refused", state: api.NamespaceStateSuspended, wantErr: errShardNamespaceClosed},
		{name: "deleting is refused", state: api.NamespaceStateDeleting, wantErr: errShardNamespaceClosed},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, idx := dbForReopen(t, class, existerWithState(t, tc.state))
			idx.shards.Store("t1", NewMockShardLike(t))

			err := db.ReopenShard(ctx, class, "t1")
			if tc.wantErr != nil {
				// errShardNamespaceClosed rather than ErrNamespaceSuspended is what
				// says this went in as a resume and not as a request.
				require.ErrorIs(t, err, tc.wantErr)
				require.NotErrorIs(t, err, namespaces.ErrNamespaceSuspended)
				return
			}
			require.NoError(t, err)
		})
	}

	// A broken lookup and a namespace that keeps no shards open are both refusals,
	// but only one of them is an operator's cue to look at the namespace map.
	t.Run("an unknown namespace is named as such, not as a closed namespace", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false).Maybe()
		db, idx := dbForReopen(t, class, e)
		idx.shards.Store("t1", NewMockShardLike(t))

		err := db.ReopenShard(ctx, class, "t1")
		require.ErrorIs(t, err, errNamespaceUnknownLocally)
		require.NotErrorIs(t, err, errShardNamespaceClosed)
	})

	// The split that goes red if the two accessors are ever collapsed into one.
	t.Run("resuming reopens while the request path is refused", func(t *testing.T) {
		db, idx := dbForReopen(t, class, existerWithState(t, api.NamespaceStateResuming))
		idx.shards.Store("t1", NewMockShardLike(t))

		require.NoError(t, db.ReopenShard(ctx, class, "t1"))
		require.ErrorIs(t, idx.LoadLocalShard(ctx, "t1", false), namespaces.ErrNamespaceResuming)
	})

	t.Run("a missing index is an error, not a silent success", func(t *testing.T) {
		db, _ := dbForReopen(t, class, existerWithState(t, api.NamespaceStateResuming))

		require.Error(t, db.ReopenShard(ctx, "alpha:Other", "t1"))
	})

	// A resuming namespace refuses requests, so a shard left registered-but-cold
	// would have nothing left to load it. The reopen therefore has to force the
	// load rather than register a lazy placeholder. The injected failure is only
	// reachable from inside Load, so it is what says the load was entered: were
	// the reopen to stop forcing, this would return nil instead.
	t.Run("a resident lazy shard is forced to load", func(t *testing.T) {
		db, idx := dbForReopen(t, class, existerWithState(t, api.NamespaceStateResuming))
		idx.shards.Store("t1", &LazyLoadShard{memMonitor: failingAllocChecker{}})

		require.ErrorIs(t, db.ReopenShard(ctx, class, "t1"), errInjectedMemoryPressure)
	})
}
