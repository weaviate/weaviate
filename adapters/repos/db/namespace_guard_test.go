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
