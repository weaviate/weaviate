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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestFilterableToRangeableStrategy_OnMigrationComplete_LocalOnly pins
// OnMigrationComplete being local-only: it may set
// Shard.rangeableLocalReady, but must never issue a cluster-wide RAFT
// schema update (GH weaviate/weaviate#12189).
func TestFilterableToRangeableStrategy_OnMigrationComplete_LocalOnly(t *testing.T) {
	const propName = "score"
	ctx := testCtx()
	className := "FilterableToRangeableLocalOnly_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	// Pre-condition: nothing has marked this property ready yet (no
	// explicit map entry, no rangeable bucket on disk pre-migration).
	require.False(t, shard.IsRangeableLocallyReady(propName),
		"pre-migration: property should not be locally ready (no bucket, no explicit entry)")

	strategy := &FilterableToRangeableStrategy{propNames: []string{propName}, generation: 1}

	err := strategy.OnMigrationComplete(ctx, shard)
	require.NoError(t, err,
		"OnMigrationComplete must succeed with no schema manager wired up - "+
			"it must not attempt any cluster-wide RAFT call")

	require.True(t, shard.IsRangeableLocallyReady(propName),
		"OnMigrationComplete must still mark the property locally ready on this shard")
}

// TestFilterableToRangeableStrategy_OnMigrationComplete_MultiProp is the
// multi-property variant of the LocalOnly test above.
func TestFilterableToRangeableStrategy_OnMigrationComplete_MultiProp(t *testing.T) {
	ctx := testCtx()
	className := "FilterableToRangeableLocalOnlyMulti_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)
	// Add a second numeric property so the migration can target both.
	secondProp := "score2"
	class.Properties = append(class.Properties, &models.Property{
		Name:     secondProp,
		DataType: []string{"int"},
	})

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	strategy := &FilterableToRangeableStrategy{
		propNames:  []string{filterableToRangeablePropName, secondProp},
		generation: 1,
	}

	require.NoError(t, strategy.OnMigrationComplete(ctx, shard))

	require.True(t, shard.IsRangeableLocallyReady(filterableToRangeablePropName))
	require.True(t, shard.IsRangeableLocallyReady(secondProp))
}
