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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/sharding/config"
)

func TestScalerScale(t *testing.T) {
	ctx := context.Background()
	t.Run("NoShardingState", func(t *testing.T) {
		f := newFakeFactory()
		f.ShardingState.M = nil
		scaler := f.Scaler()
		old := config.Config{}
		_, err := scaler.Scale(ctx, "C", old, 1, 2)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "no sharding state")
	})
	t.Run("SameReplicationFactor", func(t *testing.T) {
		scaler := newFakeFactory().Scaler()
		old := config.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 2)
		assert.Nil(t, err)
	})
	t.Run("ScaleInNotSupported", func(t *testing.T) {
		scaler := newFakeFactory().Scaler()
		old := config.Config{}
		_, err := scaler.Scale(ctx, "C", old, 2, 1)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "not supported")
	})
}

func TestScalerScaleOut(t *testing.T) {
	var (
		ctx = context.Background()
		cls = "C"
		old = config.Config{}
	)

	t.Run("Failure creating remote shards", func(t *testing.T) {
		f := newFakeFactory()

		f.Client.On("CreateShard", anyVal, anyVal, anyVal, anyVal).Return(errAny)

		scaler := f.Scaler()
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.ErrorIs(t, err, errAny)
	})

	t.Run("Success", func(t *testing.T) {
		f := newFakeFactory()

		f.Client.On("CreateShard", anyVal, "H1", cls, "S3").Return(nil)
		f.Client.On("CreateShard", anyVal, "H2", cls, "S1").Return(nil)
		f.Client.On("CreateShard", anyVal, "H3", cls, "S1").Return(nil)

		scaler := f.Scaler()
		_, err := scaler.Scale(ctx, "C", old, 1, 3)
		assert.Nil(t, err)
	})
}
