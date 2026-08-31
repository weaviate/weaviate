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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestDisablingAnIndexUnderAnArmedMirrorKeepsWritesWorking pins the teardown
// that removed a live mirror's directory without disarming it first.
//
// The sequence is an ordinary one: an enable-filterable run does not finish,
// and the operator disables the index. That schema update removes the staged
// directory while the staged bucket is still open and its mirror still armed,
// after which the mirror writes into a path that no longer exists and every
// write carrying that property fails until the process restarts.
//
// The assertion is a write and a query, never a directory: asserting the
// directory is gone passes on the broken build, because removing it is exactly
// what the broken build does.
func TestDisablingAnIndexUnderAnArmedMirrorKeepsWritesWorking(t *testing.T) {
	const propName = "subtitle"

	ctx := testCtx()
	className := "MirrorTeardown_" + uuid.NewString()[:8]
	class := newEnableFilterableTestClass(className, propName)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	put := func(text string) error {
		return shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      className,
				Properties: map[string]interface{}{propName: text},
			},
		})
	}
	require.NoError(t, put("alpha bravo charlie"))

	// A run that armed its mirror and never flipped.
	task, _ := newEnableFilterableTask(t, idx, className, propName)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.Equal(t, 1, shard.migrationMirrors.ArmedMigrationMirrors(),
		"fixture: the mirror has to be armed, or there is nothing to tear down under")

	// The operator disables the index after that run.
	require.NoError(t, idx.updateProperty(ctx, &models.Property{
		Name:            propName,
		DataType:        schema.DataTypeText.PropString(),
		Tokenization:    models.PropertyTokenizationWord,
		IndexFilterable: boolPtr(false),
		IndexSearchable: boolPtr(true),
	}))

	require.NoError(t, put("delta echo foxtrot"),
		"a write carrying the mirrored property must still succeed after the teardown")
	require.NoError(t, put("golf hotel india"))

	// And the property's remaining index really took those writes.
	searchable := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(propName))
	require.NotNil(t, searchable)
	terms := fingerprintInvertedBucket(t, searchable)
	for _, term := range []string{"alpha", "delta", "echo", "golf", "india"} {
		require.NotEmptyf(t, terms[term], "the searchable index lost term %q", term)
	}

	require.Zero(t, shard.migrationMirrors.ArmedMigrationMirrors(),
		"a teardown that removed the mirror's directory must have disarmed it")
}
