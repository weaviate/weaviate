package spfresh

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type HnswIndex struct {
	quantizer *compressionhelpers.RotationalQuantizer
	distancer *Distancer
	metrics   *Metrics
	hnsw      *hnsw.HNSW
	centroids *common.PagedArray[atomic.Pointer[Centroid]]
	counter   atomic.Int32
}

func NewHnswIndex(metrics *Metrics, store *lsmkv.Store, pages, pageSize uint64) (*HnswIndex, error) {
	index := HnswIndex{
		metrics:   metrics,
		centroids: common.NewPagedArray[atomic.Pointer[Centroid]](pages, pageSize),
	}

	cfg := hnsw.Config{
		RootPath:              "nonexistent",
		ID:                    "spfresh",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			centroid := index.Get(id)
			if centroid == nil {
				return nil, errors.New("not found")
			}
			return centroid.Uncompressed, nil
		},
	}

	ecfg := ent.UserConfig{}

	ecfg.SetDefaults()

	h, err := hnsw.New(cfg, ecfg, cyclemanager.NewCallbackGroupNoop(), store)
	if err != nil {
		return nil, err
	}

	index.hnsw = h

	return &index, nil
}

func (s *HnswIndex) Init(dims int32, distancer distancer.Provider) {
	// TODO: seed
	seed := uint64(42)
	s.quantizer = compressionhelpers.NewRotationalQuantizer(int(dims), seed, 8, distancer)
	s.distancer = &Distancer{
		quantizer: s.quantizer,
		distancer: distancer,
	}
}

func (s *HnswIndex) Get(id uint64) *Centroid {
	page, slot := s.centroids.GetPageFor(id)
	if page == nil {
		return nil
	}

	return page[slot].Load()
}

func (s *HnswIndex) Insert(id uint64, centroid *Centroid) error {
	page, slot := s.centroids.EnsurePageFor(id)
	if page == nil {
		return errors.New("failed to allocate page")
	}

	page[slot].Store(centroid)

	err := s.hnsw.Add(context.Background(), id, centroid.Uncompressed)
	if err != nil {
		return errors.Wrap(err, "add to hnsw")
	}

	s.metrics.SetPostings(int(s.counter.Add(1)))

	return nil
}

func (s *HnswIndex) MarkAsDeleted(id uint64) error {
	for {
		page, slot := s.centroids.GetPageFor(id)
		if page == nil {
			return nil
		}
		centroid := page[slot].Load()
		if centroid == nil {
			return errors.New("centroid not found")
		}

		if centroid.Deleted {
			return errors.New("centroid already marked as deleted")
		}

		newCentroid := Centroid{
			Uncompressed: centroid.Uncompressed,
			Compressed:   centroid.Compressed,
			Deleted:      true,
		}

		if page[slot].CompareAndSwap(centroid, &newCentroid) {
			s.metrics.SetPostings(int(s.counter.Add(-1)))
			break
		}
	}

	return s.hnsw.Delete(id)
}

func (s *HnswIndex) Exists(id uint64) bool {
	centroid := s.Get(id)
	if centroid == nil {
		return false
	}

	return !centroid.Deleted
}

func (s *HnswIndex) Quantizer() *compressionhelpers.RotationalQuantizer {
	return s.quantizer
}

func (s *HnswIndex) Search(query []float32, k int) (*ResultSet, error) {
	start := time.Now()
	defer s.metrics.CentroidSearchDuration(start)

	ids, distances, err := s.hnsw.SearchByVector(context.TODO(), query, k, nil)
	if err != nil {
		return nil, err
	}

	results := make([]Result, len(ids))
	for i := range ids {
		results[i] = Result{ID: ids[i], Distance: distances[i]}
	}

	return &ResultSet{data: results}, nil
}
