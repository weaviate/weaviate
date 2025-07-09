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

package hnsw

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// Config for a new HSNW index, this contains information that is derived
// internally, e.g. by the shard. All User-settable config is specified in
// Config.UserConfig
type Config struct {
	// internal
	RootPath                  string
	ID                        string
	MakeCommitLoggerThunk     MakeCommitLogger
	VectorForIDThunk          common.VectorForID[float32]
	MultiVectorForIDThunk     common.VectorForID[[]float32]
	TempVectorForIDThunk      common.TempVectorForID[float32]
	TempMultiVectorForIDThunk common.TempVectorForID[[]float32]
	Logger                    logrus.FieldLogger
	DistanceProvider          distancer.Provider
	PrometheusMetrics         *monitoring.PrometheusMetrics
	AllocChecker              memwatch.AllocChecker
	WaitForCachePrefill       bool
	FlatSearchConcurrency     int
	AcornFilterRatio          float64
	DisableSnapshots          bool
	SnapshotOnStartup         bool
	LazyLoadSegments          bool

	// metadata for monitoring
	ShardName string
	ClassName string

	VisitedListPoolMaxSize int
}

func (c Config) Validate() error {
	ec := errorcompounder.New()

	if c.ID == "" {
		ec.Addf("id cannot be empty")
	}

	if c.RootPath == "" {
		ec.Addf("rootPath cannot be empty")
	}

	if c.MakeCommitLoggerThunk == nil {
		ec.Addf("makeCommitLoggerThunk cannot be nil")
	}

	if c.VectorForIDThunk == nil {
		ec.Addf("vectorForIDThunk cannot be nil")
	}

	if c.DistanceProvider == nil {
		ec.Addf("distancerProvider cannot be nil")
	}

	return ec.ToError()
}

func NewVectorForIDThunk[T float32 | []float32](targetVector string, fn func(ctx context.Context, id uint64, targetVector string) ([]T, error)) common.VectorForID[T] {
	t := common.TargetVectorForID[T]{
		TargetVector:     targetVector,
		VectorForIDThunk: fn,
	}
	return t.VectorForID
}

func NewMultiVectorForIDThunk(targetVector string, fn func(ctx context.Context, id uint64, targetVector string) ([][]float32, error)) common.VectorForID[[]float32] {
	t := common.TargetVectorForID[[]float32]{
		TargetVector:     targetVector,
		VectorForIDThunk: fn,
	}
	return t.VectorForID
}

func NewTempVectorForIDThunk[T float32 | []float32](targetVector string, fn func(ctx context.Context, indexID uint64, container *common.VectorSlice, targetVector string) ([]T, error)) common.TempVectorForID[T] {
	t := common.TargetTempVectorForID[T]{
		TargetVector:         targetVector,
		TempVectorForIDThunk: fn,
	}
	return t.TempVectorForID
}

func NewTempMultiVectorForIDThunk(targetVector string, fn func(ctx context.Context, indexID uint64, container *common.VectorSlice, targetVector string) ([][]float32, error)) common.TempVectorForID[[]float32] {
	t := common.TargetTempVectorForID[[]float32]{
		TargetVector:         targetVector,
		TempVectorForIDThunk: fn,
	}
	return t.TempVectorForID
}
