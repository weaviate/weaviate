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
	"encoding/binary"
	"io"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
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
	// vectors are the centroids, by posting, each its own allocation: that
	// of a retired posting is nil (release), so that the memory of the
	// centroids follows the live postings. Whoever changes vectors holds the
	// lock of the index they belong to and mu; whoever reads them holds one
	// of the two.
	vectors [][]float32
	// mu is for the graph of an index on disk, which reads a centroid from
	// the goroutines of the index, which hold its lock, and from others that
	// hold none: the one that fills its cache once it is opened, and the
	// cycle of the node that takes its deleted centroids out.
	mu    sync.RWMutex
	graph *hnsw.HNSW
	// codes tells that the graph keeps the centroids as 8-bit RQ codes, as
	// HFresh keeps its own, and that nothing of them is in float32 in memory:
	// vectors is empty, a centroid is what the graph decodes from its code,
	// and file is where the graph reads one from while it converts to codes
	// or is built again.
	codes bool
	file  *centroidFile
	// store and bucket are where a graph with codes keeps them: a bucket of
	// a store the index does not own, which closes with the graph.
	store  *lsmkv.Store
	bucket string
}

// appendVectors adds the centroids of new postings. Whoever calls it holds
// the lock of the index, and adds them to the graph after. A graph with codes
// keeps them itself.
func (c *centroidIndex) appendVectors(vectors ...[]float32) {
	if c.codes {
		return
	}
	c.mu.Lock()
	c.vectors = append(c.vectors, vectors...)
	c.mu.Unlock()
}

// release lets the centroid of a retired posting go, once the graph has
// deleted it. The graph may still hold it in its cache, with its tombstone,
// until its cleanup takes the node out of the cache and, with codes, out of
// the bucket of the codes. Whoever calls it holds the lock of the index.
func (c *centroidIndex) release(p uint32) {
	if c.codes {
		return
	}
	c.mu.Lock()
	if int(p) < len(c.vectors) {
		c.vectors[p] = nil
	}
	c.mu.Unlock()
}

// vector is the centroid of a live posting, for whoever holds the lock of
// the index and asks the graph about it: with codes, the one the graph
// decodes from its code.
func (c *centroidIndex) vector(p uint32) ([]float32, error) {
	if c.codes {
		vec, err := c.graph.Get(uint64(p))
		return vec, errors.Wrapf(err, "decode the centroid of posting %d", p)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if int(p) >= len(c.vectors) || c.vectors[p] == nil {
		return nil, storobj.NewErrNotFoundf(uint64(p), "no centroid held for the posting")
	}
	return c.vectors[p], nil
}

// publishedVector is how the graph of an index on disk reads a centroid:
// from vectors, or with codes from the file of the centroids.
func (c *centroidIndex) publishedVector(_ context.Context, id uint64) ([]float32, error) {
	if c.codes {
		return c.file.read(id)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if id >= uint64(len(c.vectors)) {
		return nil, storobj.NewErrNotFoundf(id, "centroid out of range")
	}
	if c.vectors[id] == nil {
		return nil, storobj.NewErrNotFoundf(id, "the centroid of a retired posting")
	}
	return c.vectors[id], nil
}

// centroidFile reads one centroid at a time from the file of the centroids,
// which keeps every posting id given out in float32.
type centroidFile struct {
	file *os.File
	dims int
}

func openCentroidFile(path string, dims int) (*centroidFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open the file of the centroids to read")
	}
	return &centroidFile{file: file, dims: dims}, nil
}

// read reads the centroid of a posting id, in a slice of its own.
func (f *centroidFile) read(id uint64) ([]float32, error) {
	buf := make([]byte, 4*f.dims)
	if _, err := f.file.ReadAt(buf, int64(id)*int64(len(buf))); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, storobj.NewErrNotFoundf(id, "centroid out of range")
		}
		return nil, errors.Wrapf(err, "read centroid %d", id)
	}
	vec := make([]float32, f.dims)
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
	}
	return vec, nil
}

func (f *centroidFile) close() error {
	if f == nil {
		return nil
	}
	return f.file.Close()
}

// newCentroidIndex builds the graph over the centroids. dir is where the hnsw
// package keeps its files; with the no-op commit logger used here it only
// creates an empty directory there. The graph reads the centroids through
// the index, so a centroid appended to its vectors can be added to it.
func newCentroidIndex(ctx context.Context, cfg *Config, dir string, vectors [][]float32,
) (*centroidIndex, error) {
	index := &centroidIndex{vectors: vectors}
	provider := distancer.Provider(distancer.NewL2SquaredProvider())
	if cfg.centroidProvider != nil {
		provider = cfg.centroidProvider
	}
	vectorForID := func(_ context.Context, id uint64) ([]float32, error) {
		if id >= uint64(len(index.vectors)) {
			return nil, storobj.NewErrNotFoundf(id, "centroid out of range")
		}
		return index.vectors[id], nil
	}

	graph, err := hnsw.New(hnsw.Config{
		RootPath:              dir,
		ID:                    "centroids",
		ShardName:             cfg.ShardName,
		ClassName:             cfg.ClassName,
		Logger:                cfg.Logger,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      provider,
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

	index.graph = graph
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
	return c.closeCodes(ctx, c.graph.Shutdown(ctx))
}

func (c *centroidIndex) drop(ctx context.Context) error {
	return c.closeCodes(ctx, c.graph.Drop(ctx))
}

// closeCodes closes, after the graph, the file of the centroids it read and
// the bucket of its codes, and returns the first error, err's if it has one.
func (c *centroidIndex) closeCodes(ctx context.Context, err error) error {
	if closeErr := c.file.close(); err == nil {
		err = closeErr
	}
	if c.store != nil {
		if closeErr := shutdownBucket(ctx, c.store, c.bucket); err == nil && closeErr != nil {
			err = errors.Wrapf(closeErr, "close the bucket %s", c.bucket)
		}
	}
	return err
}
