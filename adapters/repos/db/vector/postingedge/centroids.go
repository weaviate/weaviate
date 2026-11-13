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

package postingedge

import (
	"context"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	entcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type noopBucketView struct{}

func (noopBucketView) ReleaseView() {}

// centroidIndex finds the postings a search starts from. It is an HNSW graph
// over the centroid vectors, uncompressed and in l2 space, the space the
// vectors were assigned to their centroids in. Graph node ids are posting
// ids.
type centroidIndex struct {
	vectors [][]float32
	graph   *hnsw.HNSW
}

// newCentroidIndex builds the graph over the centroids. dir is where the hnsw
// package keeps its files; with the no-op commit logger used here it only
// creates an empty directory there.
func newCentroidIndex(ctx context.Context, cfg *Config, dir string, vectors [][]float32,
) (*centroidIndex, error) {
	vectorForID := func(_ context.Context, id uint64) ([]float32, error) {
		if id >= uint64(len(vectors)) {
			return nil, storobj.NewErrNotFoundf(id, "centroid out of range")
		}
		return vectors[id], nil
	}

	graph, err := hnsw.New(hnsw.Config{
		RootPath:              dir,
		ID:                    "centroids",
		ShardName:             cfg.ShardName,
		ClassName:             cfg.ClassName,
		Logger:                cfg.Logger,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		AllocChecker:          cfg.AllocChecker,
		VectorForIDThunk:      vectorForID,
		TempVectorForIDWithViewThunk: func(ctx context.Context, id uint64, container *common.VectorSlice,
			_ common.BucketView,
		) ([]float32, error) {
			vec, err := vectorForID(ctx, id)
			if err != nil {
				return nil, err
			}
			container.Slice = append(container.Slice[:0], vec...)
			return container.Slice, nil
		},
		GetViewThunk: func() common.BucketView { return noopBucketView{} },
		// Marks the graph as a component of another index for metrics, as
		// HFresh does for its centroid graph.
		HFreshMode: true,
	}, centroidUserConfig(centroidEF), cyclemanager.NewCallbackGroupNoop(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "create centroid graph")
	}
	graph.PostStartup(ctx)

	index := &centroidIndex{vectors: vectors, graph: graph}
	err = parallelChunks(ctx, cfg.Logger, len(vectors), 16, func(_, start, end int) error {
		for id := start; id < end; id++ {
			if err := graph.Add(ctx, uint64(id), vectors[id]); err != nil {
				return errors.Wrapf(err, "add centroid %d", id)
			}
		}
		return nil
	})
	if err != nil {
		// The half-built graph is discarded; its shutdown error would only
		// hide the one that matters.
		_ = graph.Shutdown(ctx)
		return nil, err
	}
	return index, nil
}

// centroidUserConfig is the configuration of the centroid graph, searched with
// the given ef.
func centroidUserConfig(ef int) enthnsw.UserConfig {
	uc := enthnsw.UserConfig{}
	uc.SetDefaults()
	uc.Distance = entcommon.DistanceL2Squared
	uc.EF = ef
	return uc
}

// nearest returns the ids of the postings whose centroids are nearest to the
// query, nearest first.
func (c *centroidIndex) nearest(ctx context.Context, query []float32, limit int) ([]uint64, error) {
	ids, _, err := c.graph.SearchByVector(ctx, query, limit, nil)
	if err != nil {
		return nil, errors.Wrap(err, "search centroid graph")
	}
	return ids, nil
}

func (c *centroidIndex) shutdown(ctx context.Context) error {
	return c.graph.Shutdown(ctx)
}

func (c *centroidIndex) drop(ctx context.Context, keepFiles bool) error {
	return c.graph.Drop(ctx, keepFiles)
}
