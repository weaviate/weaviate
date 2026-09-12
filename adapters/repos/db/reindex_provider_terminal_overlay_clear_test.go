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
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// After a FAILED migration no schema flip follows, so a later index delete
// sweeps the very bucket the overlay forces writes into.
func TestWriteAfterFailedMigrationSweepsOverlaidBucket(t *testing.T) {
	ctx := testCtx()
	className := "FailedOverlaySweep_" + uuid.NewString()[:8]
	const prop = "p"

	class := newTestClassWithProps(className, []string{prop})
	class.Properties[0].IndexFilterable = boolPtr(false)
	class.Properties[0].IndexSearchable = boolPtr(true)

	hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	defer hot.Shutdown(context.Background())
	shard, err := unwrapShard(ctx, hot)
	require.NoError(t, err)

	require.NoError(t, shard.store.CreateOrLoadBucket(ctx,
		helpers.BucketFromPropNameLSM(prop), lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	shard.SetPropertyOverlay(prop, inverted.PropertyOverlay{ForceFilterable: true})

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:    className,
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{prop},
		UnitToShard:   map[string]string{"u1": hot.Name()},
	})
	require.NoError(t, err)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, ctx)
	require.NoError(t, p.OnTaskCompleted(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_failed", Version: 1},
		Status:         distributedtask.TaskStatusFailed,
		Payload:        payload,
	}))

	// The delete drops every bucket the schema says is off, including the
	// one the failed migration left behind.
	class.Properties[0].IndexSearchable = boolPtr(false)
	eg := enterrors.NewErrorGroupWrapper(shard.index.logger)
	var reads atomic.Int64
	shard.updatePropertyBuckets(ctx, eg, class.Properties[0], &reads)
	require.NoError(t, eg.Wait())
	require.Nil(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(prop)),
		"the sweep must remove the bucket the overlay points at")

	obj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID(uuid.NewString()),
			Class:      className,
			Properties: map[string]interface{}{prop: "alpha bravo"},
		},
	}
	require.NoError(t, shard.PutObject(ctx, obj),
		"a write after the sweep must not demand the removed bucket")

	analyzed, _, _, err := shard.AnalyzeObject(obj)
	require.NoError(t, err)
	names := make([]string, 0, len(analyzed))
	for _, a := range analyzed {
		names = append(names, a.Name)
	}
	require.NotEmpty(t, names, "analysis still runs; only this property drops out")
	assert.NotContains(t, names, prop,
		"the schema indexes the property nowhere, so the analyzer must skip it")
}
