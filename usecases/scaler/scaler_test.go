//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package scaler

import (
	"context"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestScalerScale(t *testing.T) {
	ctx := context.Background()
	t.Run("NoShardingState", func(t *testing.T) {
		f := newFakeFactory()
		f.ShardingState.M = nil
		scaler := f.Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 1, 2)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "no sharding state")
	})
	t.Run("SameReplicationFactor", func(t *testing.T) {
		scaler := newFakeFactory().Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 2)
		assert.Nil(t, err)
	})
	t.Run("ScaleInNotSupported", func(t *testing.T) {
		scaler := newFakeFactory().Scaler("")
		old := sharding.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})
}

func TestScalerScaleOut(t *testing.T) {
	var (
		dataDir = t.TempDir()
		ctx     = context.Background()
		cls     = "C"
		old     = sharding.Config{}
		bak     = backup.ClassDescriptor{
			Name: "C",
			Shards: []*backup.ShardDescriptor{
				{
					Name: "S1", Files: []string{"f1"},
					PropLengthTrackerPath: "f4",
					ShardVersionPath:      "f4",
					DocIDCounterPath:      "f4",
				},
			},
		}
	)
	for i := 1; i < 5; i++ {
		file, err := os.Create(path.Join(dataDir, "f"+strconv.Itoa(i)))
		assert.Nil(t, err)
		file.Close()
	}
	t.Run("UnresolvedName", func(t *testing.T) {
		f := newFakeFactory()
		delete(f.NodeHostMap, "N3")
		scaler := f.Scaler(dataDir)
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.ErrorIs(t, err, ErrUnresolvedName)
	})
	t.Run("GetLocalShards", func(t *testing.T) {
		f := newFakeFactory()
		f.Source.On("ShardsBackup", anyVal, anyVal, cls, []string{"S1"}).Return(bak, errAny)

		// shard doesn't exist locally
		f.Client.On("IncreaseReplicationFactor", anyVal, "H3", cls, anyVal, anyVal).Return(nil)
		f.Client.On("IncreaseReplicationFactor", anyVal, "H4", cls, anyVal, anyVal).Return(nil)

		f.Source.On("ReleaseBackup", anyVal, anyVal, "C").Return(nil)
		scaler := f.Scaler(dataDir)
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.ErrorIs(t, err, errAny)
	})

	t.Run("IncreaseFactor", func(t *testing.T) {
		f := newFakeFactory()
		f.Source.On("ShardsBackup", anyVal, anyVal, cls, []string{"S1"}).Return(bak, nil)
		// sync update to remote node N2
		f.Client.On("CreateShard", anyVal, "H2", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H2", cls, "S1").Return(nil)

		// sync update to remote node N3
		f.Client.On("CreateShard", anyVal, "H3", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H3", cls, "S1").Return(nil)

		// shard doesn't exist locally
		f.Client.On("IncreaseReplicationFactor", anyVal, "H3", cls, anyVal, anyVal).Return(errAny)
		f.Client.On("IncreaseReplicationFactor", anyVal, "H4", cls, anyVal, anyVal).Return(nil)

		f.Source.On("ReleaseBackup", anyVal, anyVal, "C").Return(nil)
		scaler := f.Scaler(dataDir)
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.ErrorIs(t, err, errAny)
	})

	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory()
		f.Source.On("ShardsBackup", anyVal, anyVal, cls, []string{"S1"}).Return(bak, nil)
		// sync update to remote node N2
		f.Client.On("CreateShard", anyVal, "H2", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H2", cls, "S1").Return(nil)

		// sync update to remote node N3
		f.Client.On("CreateShard", anyVal, "H3", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H3", cls, "S1").Return(nil)

		// shard doesn't exist locally
		f.Client.On("IncreaseReplicationFactor", anyVal, "H3", cls, anyVal, anyVal).Return(nil)
		f.Client.On("IncreaseReplicationFactor", anyVal, "H4", cls, anyVal, anyVal).Return(nil)

		f.Source.On("ReleaseBackup", anyVal, anyVal, "C").Return(nil)
		scaler := f.Scaler(dataDir)
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.Nil(t, err)
	})

	t.Run("ReleaseBackupAsync", func(t *testing.T) {
		f := newFakeFactory()
		f.Source.On("ShardsBackup", anyVal, anyVal, cls, []string{"S1"}).Return(bak, nil)
		// sync update to remote node N2
		f.Client.On("CreateShard", anyVal, "H2", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H2", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H2", cls, "S1").Return(nil)

		// sync update to remote node N3
		f.Client.On("CreateShard", anyVal, "H3", cls, "S1").Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f1", anyVal).Return(nil)
		f.Client.On("PutFile", anyVal, "H3", cls, "S1", "f4", anyVal).Return(nil)
		f.Client.On("ReInitShard", anyVal, "H3", cls, "S1").Return(nil)

		// shard doesn't exist locally
		f.Client.On("IncreaseReplicationFactor", anyVal, "H3", cls, anyVal, anyVal).Return(nil)
		f.Client.On("IncreaseReplicationFactor", anyVal, "H4", cls, anyVal, anyVal).Return(nil)

		f.Source.On("ReleaseBackup", anyVal, anyVal, "C").Return(errAny)
		scaler := f.Scaler(dataDir)
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.Nil(t, err)
	})
}
