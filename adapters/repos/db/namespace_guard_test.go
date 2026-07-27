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

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/namespaces"
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
func newIndexForNamespaceTest(t *testing.T, className string, e namespaces.Exister) *Index {
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
		sg, reader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Shutdown(context.Background()) })
	return idx
}

func TestNamespaceGuard(t *testing.T) {
	t.Run("the owning namespace's state reaches both accessors", func(t *testing.T) {
		tests := []struct {
			name         string
			state        api.NamespaceState
			shouldBeOpen bool
			loadableErr  error
		}{
			{name: "active", state: api.NamespaceStateActive, shouldBeOpen: true},
			{name: "suspended", state: api.NamespaceStateSuspended, loadableErr: namespaces.ErrNamespaceSuspended},
			{name: "resuming", state: api.NamespaceStateResuming, shouldBeOpen: true, loadableErr: namespaces.ErrNamespaceResuming},
			{name: "deleting", state: api.NamespaceStateDeleting, loadableErr: namespaces.ErrNamespaceDeleting},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				e := namespaces.NewMockExister(t)
				e.EXPECT().GetNamespace("alpha").
					Return(api.Namespace{Name: "alpha", State: tc.state}, true)

				idx, _ := indexForNamespace(t, "alpha:Product", e)

				assert.Equal(t, tc.shouldBeOpen, idx.shardsShouldBeOpen())
				if tc.loadableErr != nil {
					require.ErrorIs(t, idx.requireShardLoadable(), tc.loadableErr)
				} else {
					require.NoError(t, idx.requireShardLoadable())
				}
			})
		}
	})

	// A namespaced class with no lookup and one whose namespace is absent both
	// refuse, but for reasons an operator must be able to tell apart: the first
	// is a lost wiring line, the second a broken invariant.
	t.Run("a namespaced class refuses on a missing lookup and on a missing namespace", func(t *testing.T) {
		noLookup, noLookupHook := indexForNamespace(t, "alpha:Product", nil)
		assert.False(t, noLookup.shardsShouldBeOpen())
		require.ErrorIs(t, noLookup.requireShardLoadable(), errNamespaceLookupMissing)
		require.NotNil(t, noLookupHook.LastEntry(), "a missing lookup must be logged")

		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		miss, _ := indexForNamespace(t, "alpha:Product", e)

		assert.False(t, miss.shardsShouldBeOpen())
		require.ErrorIs(t, miss.requireShardLoadable(), errNamespaceRowMissing)
		require.NotErrorIs(t, miss.requireShardLoadable(), errNamespaceLookupMissing)
	})

	// An unqualified class name is the only un-namespaced case, so it is the only
	// one that may skip the lookup and be admitted.
	t.Run("an unqualified class name is admitted with no lookup at all", func(t *testing.T) {
		idx, _ := indexForNamespace(t, "Product", nil)

		require.Empty(t, idx.namespace)
		assert.True(t, idx.shardsShouldBeOpen())
		require.NoError(t, idx.requireShardLoadable())
	})

	// NewMockExister fails the test on an unexpected call, so registering no
	// expectation asserts an unqualified name never reaches the lookup.
	t.Run("an unqualified class name does not consult the lookup it has", func(t *testing.T) {
		idx, _ := indexForNamespace(t, "Product", namespaces.NewMockExister(t))

		assert.True(t, idx.shardsShouldBeOpen())
		require.NoError(t, idx.requireShardLoadable())
	})

	t.Run("a lookup miss is logged at Error with class and namespace", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		idx, hook := indexForNamespace(t, "alpha:Product", e)

		require.ErrorIs(t, idx.requireShardLoadable(), errNamespaceRowMissing)

		entry := hook.LastEntry()
		require.NotNil(t, entry, "a lookup miss must be logged")
		assert.Equal(t, logrus.ErrorLevel, entry.Level)
		assert.Equal(t, "alpha:Product", entry.Data["class"])
		assert.Equal(t, "alpha", entry.Data["namespace"])
		assert.Contains(t, entry.Message, errNamespaceRowMissing.Error())
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
			e := namespaces.NewMockExister(t)
			idx := newIndexForNamespaceTest(t, tc.className, e)

			require.Equal(t, tc.wantNamespace, idx.namespace,
				"NewIndex must parse the owning namespace out of the class name")
			require.Same(t, e, idx.namespacesExister,
				"NewIndex must carry the exister from its config")
		})
	}
}

// The empty cell is the one that matters: boot treats a missing tenant status as
// HOT, so a single-tenant shard — which carries none — must stay desired-open.
func TestDesiredOpen(t *testing.T) {
	states := []struct {
		name  string
		state api.NamespaceState
		open  bool
	}{
		{name: "active", state: api.NamespaceStateActive, open: true},
		{name: "resuming", state: api.NamespaceStateResuming, open: true},
		{name: "suspended", state: api.NamespaceStateSuspended},
		{name: "deleting", state: api.NamespaceStateDeleting},
		{name: "unknown", state: api.NamespaceState("")},
	}
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

	for _, s := range states {
		for _, st := range statuses {
			t.Run("multi-tenant/"+s.name+"/"+st.name, func(t *testing.T) {
				assert.Equal(t, s.open && st.hot, desiredOpen(s.state, true, st.status))
			})
		}
	}

	// A single-tenant shard carries no tenant, so no status may close it.
	for _, s := range states {
		for _, st := range statuses {
			t.Run("single-tenant/"+s.name+"/"+st.name, func(t *testing.T) {
				assert.Equal(t, s.open, desiredOpen(s.state, false, st.status))
			})
		}
	}

	t.Run("an empty tenant status decides exactly as HOT", func(t *testing.T) {
		for _, s := range states {
			assert.Equal(t, desiredOpen(s.state, true, models.TenantActivityStatusHOT),
				desiredOpen(s.state, true, ""), "state %q", s.state)
		}
	})
}

// dbForDesiredOpen builds the minimum DB DesiredOpenLocalShards reads: a schema
// reader over one class's sharding state, plus the namespace lookup.
func dbForDesiredOpen(t *testing.T, className string, e namespaces.Exister, partitioningEnabled bool, shards map[string]sharding.Physical) *DB {
	t.Helper()

	logger, _ := logrustest.NewNullLogger()
	state := &sharding.State{Physical: shards, PartitioningEnabled: partitioningEnabled}
	state.SetLocalName("node1")

	reader := schemaUC.NewMockSchemaReader(t)
	reader.EXPECT().Read(className, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, state)
		}).Maybe()

	return &DB{logger: logger, schemaReader: reader, namespacesExister: e}
}

func localShard(name string) sharding.Physical {
	return sharding.Physical{Name: name, BelongsToNodes: []string{"node1"}}
}

func coldShard(name string) sharding.Physical {
	p := localShard(name)
	p.Status = models.TenantActivityStatusCOLD
	return p
}

func TestDesiredOpenLocalShards(t *testing.T) {
	const class = "alpha:Product"

	existerFor := func(t *testing.T, state api.NamespaceState) namespaces.Exister {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").
			Return(api.Namespace{Name: "alpha", State: state}, true).Maybe()
		return e
	}

	tests := []struct {
		name   string
		state  api.NamespaceState
		shards map[string]sharding.Physical
		want   []string
	}{
		{
			name:   "no local shards yields an empty set",
			state:  api.NamespaceStateActive,
			shards: map[string]sharding.Physical{"s1": {Name: "s1", BelongsToNodes: []string{"other"}}},
			want:   nil,
		},
		{
			name:   "one HOT shard yields that shard",
			state:  api.NamespaceStateActive,
			shards: map[string]sharding.Physical{"s1": localShard("s1")},
			want:   []string{"s1"},
		},
		{
			name:  "an active class yields only its HOT shards",
			state: api.NamespaceStateActive,
			shards: map[string]sharding.Physical{
				"hot1": localShard("hot1"), "hot2": localShard("hot2"), "cold1": coldShard("cold1"),
			},
			want: []string{"hot1", "hot2"},
		},
		{
			name:  "a suspended class yields nothing",
			state: api.NamespaceStateSuspended,
			shards: map[string]sharding.Physical{
				"hot1": localShard("hot1"), "hot2": localShard("hot2"),
			},
			want: nil,
		},
		{
			// Not the empty set: the shards must reopen for the resume to finish.
			name:  "a resuming class yields its HOT shards",
			state: api.NamespaceStateResuming,
			shards: map[string]sharding.Physical{
				"hot1": localShard("hot1"), "cold1": coldShard("cold1"),
			},
			want: []string{"hot1"},
		},
		{
			name:   "a deleting class yields nothing",
			state:  api.NamespaceStateDeleting,
			shards: map[string]sharding.Physical{"hot1": localShard("hot1")},
			want:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := dbForDesiredOpen(t, class, existerFor(t, tc.state), true, tc.shards)

			got, err := db.DesiredOpenLocalShards(class)
			require.NoError(t, err)
			assert.ElementsMatch(t, tc.want, got)
		})
	}

	t.Run("an unqualified class yields all its local HOT shards", func(t *testing.T) {
		db := dbForDesiredOpen(t, "Product", nil, true, map[string]sharding.Physical{
			"hot1": localShard("hot1"), "cold1": coldShard("cold1"),
		})

		got, err := db.DesiredOpenLocalShards("Product")
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"hot1"}, got)
	})

	// A single-tenant class has no tenant status to filter on, so the namespace
	// alone decides — matching the single-tenant reload, which filters on nothing.
	t.Run("a single-tenant class ignores a stray shard status", func(t *testing.T) {
		db := dbForDesiredOpen(t, class, existerFor(t, api.NamespaceStateActive), false,
			map[string]sharding.Physical{"s1": localShard("s1"), "s2": coldShard("s2")})

		got, err := db.DesiredOpenLocalShards(class)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"s1", "s2"}, got)
	})

	t.Run("a suspended single-tenant class still yields nothing", func(t *testing.T) {
		db := dbForDesiredOpen(t, class, existerFor(t, api.NamespaceStateSuspended), false,
			map[string]sharding.Physical{"s1": localShard("s1")})

		got, err := db.DesiredOpenLocalShards(class)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	// Registering no Read expectation asserts the shards are never enumerated:
	// when nothing may be open the answer is empty whatever the shards are, and a
	// sweep pass must not walk every tenant to learn that.
	t.Run("a suspended class is answered without reading the sharding state", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").
			Return(api.Namespace{Name: "alpha", State: api.NamespaceStateSuspended}, true)
		logger, _ := logrustest.NewNullLogger()
		db := &DB{logger: logger, schemaReader: schemaUC.NewMockSchemaReader(t), namespacesExister: e}

		got, err := db.DesiredOpenLocalShards(class)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	// The miss must surface as an error, not as an empty set a caller would read
	// as "unload everything".
	t.Run("a lookup miss returns the error", func(t *testing.T) {
		e := namespaces.NewMockExister(t)
		e.EXPECT().GetNamespace("alpha").Return(api.Namespace{}, false)
		db := dbForDesiredOpen(t, class, e, true, map[string]sharding.Physical{"hot1": localShard("hot1")})

		got, err := db.DesiredOpenLocalShards(class)
		require.ErrorIs(t, err, errNamespaceRowMissing)
		assert.Nil(t, got)
	})
}
