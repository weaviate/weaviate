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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// weaviate_lsm_roaringsetrange_leaf_cache_config is the only series in its
// subsystem that reads on the default path, and it reads because starting a DB
// publishes it. Asserting the gauge rather than the call survives the
// publication moving inside New or into something New calls; only removing it
// goes red. Both arms run, so neither can pass on the other's leftover value.
func TestNewPublishesTheRangeableConfigGauge(t *testing.T) {
	tests := []struct {
		name              string
		rangeableInMemory bool
		want              string
	}{
		{name: "the default path", rangeableInMemory: false, want: "disabled_feature_off"},
		{name: "feature on", rangeableInMemory: true, want: "enabled"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Prime with the opposite reading so a stale gauge cannot pass.
			roaringsetrange.PublishConfig(!tt.rangeableInMemory, nil)

			repo := newTestRepo(t, tt.rangeableInMemory)
			t.Cleanup(func() { repo.Shutdown(context.Background()) })

			states := gatheredLabelValues(t, "weaviate_lsm_roaringsetrange_leaf_cache_config")
			require.NotEmptyf(t, states,
				"starting a DB published no leaf-cache config gauge, so the default "+
					"configuration is unreadable again")
			require.Equal(t, map[string]float64{
				"disabled_feature_off": boolToFloat(tt.want == "disabled_feature_off"),
				"unparseable":          0,
				"disabled_budget_zero": 0,
				"enabled":              boolToFloat(tt.want == "enabled"),
			}, states)
		})
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// gatheredLabelValues reads one gauge family from the default registry, which is
// where this subsystem registers, as {label value: reading}.
func gatheredLabelValues(t *testing.T, name string) map[string]float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		got := map[string]float64{}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				got[label.GetValue()] = metric.GetGauge().GetValue()
			}
		}
		return got
	}
	return nil
}

func newTestRepo(t *testing.T, rangeableInMemory bool) *DB {
	t.Helper()

	logger, _ := test.NewNullLogger()
	shardState := singleShardState()

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(true),
		IndexRangeableInMemory:    rangeableInMemory,
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader,
	)
	require.NoError(t, err)

	repo.SetSchemaGetter(&fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	})
	require.NoError(t, repo.WaitForStartup(testCtx()))
	return repo
}
