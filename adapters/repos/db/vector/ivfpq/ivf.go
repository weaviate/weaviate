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

package ivfpq

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/priorityqueue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	targetProbe        = 250
	rescoreConcurrency = 10
	bucketThreshold    = 1000
)

type bucket struct {
	sync.RWMutex
	code          []byte
	ids           []uint64
	codes         []uint64
	accessCounter byte
}

func (b *bucket) store(store *lsmkv.Bucket) {
	if b == nil {
		return
	}
	var buf *bytes.Buffer = new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, b.codes)
	toWrite := buf.Bytes()
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, uint64(b.code[0])*256+uint64(b.code[1]))
	store.Put(idBytes, toWrite)
	if len(b.ids) > bucketThreshold {
		b.codes = nil
	}
}

func (b *bucket) load(store *lsmkv.Bucket, sketchSize int) int {
	if b == nil || len(b.ids) < bucketThreshold {
		return 0
	}
	b.Lock()
	b.accessCounter++
	read := 0
	if b.accessCounter == 1 {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, uint64(b.code[0])*256+uint64(b.code[1]))
		buf, _ := store.Get(idBytes)
		b.codes = make([]uint64, len(b.ids)*sketchSize)
		binary.Read(bytes.NewReader(buf), binary.LittleEndian, &b.codes)
		read = len(buf)
	}
	b.Unlock()
	return read
}

func (b *bucket) release() {
	b.Lock()
	b.accessCounter--
	if b.accessCounter == 0 {
		b.codes = nil
	}
	b.Unlock()
}

func (b *bucket) memInUse() int {
	baseSize := int(reflect.TypeOf(b).Size())
	if b != nil {
		baseSize += int(reflect.TypeOf(*b).Size()) + 2 + len(b.ids)*8
	}
	if b == nil || len(b.ids) >= bucketThreshold {
		return baseSize
	}
	return len(b.codes)*8 + baseSize
}

type FlatPQ struct {
	sync.Mutex

	segments   int
	centroids  int
	probing    int
	sketchSize int

	pq      *compressionhelpers.ProductQuantizer
	bq      compressionhelpers.BinaryQuantizer
	buckets []*bucket
	locks   *common.ShardedRWLocks
	store   *lsmkv.Store

	distancer distancer.Provider
	bytesRead int
}

func NewFlatPQ(vectors [][]float32, distancer distancer.Provider, segments, centroids, probing int, store *lsmkv.Store) *FlatPQ {
	pq, _ := compressionhelpers.NewProductQuantizer(hnsw.PQConfig{
		Enabled:       true,
		Segments:      segments,
		Centroids:     centroids,
		TrainingLimit: len(vectors),
		Encoder: hnsw.PQEncoder{
			Type:         hnsw.PQEncoderTypeKMeans,
			Distribution: hnsw.PQEncoderDistributionNormal,
		},
	}, distancer, len(vectors[0]), logrus.New())
	pq.Fit(vectors)
	totalSize := int(math.Pow(float64(centroids), float64(segments)))
	i := &FlatPQ{
		segments:   segments,
		centroids:  centroids,
		probing:    probing,
		sketchSize: len(vectors[0]) / 64,
		pq:         pq,
		bq:         compressionhelpers.NewBinaryQuantizer(distancer),
		buckets:    make([]*bucket, totalSize),
		locks:      common.NewDefaultShardedRWLocks(),
		store:      store,
		distancer:  distancer,
	}

	store.CreateOrLoadBucket(context.Background(), "ivf")
	store.CreateOrLoadBucket(context.Background(), "vectors")
	return i
}

func (i *FlatPQ) Add(id uint64, vector []float32) {
	code := i.pq.Encode(vector)
	flatId := i.idFromCode(code)
	i.locks.Lock(flatId)
	if i.buckets[flatId] == nil {
		i.buckets[flatId] = &bucket{
			code: code,
		}
	}
	i.buckets[flatId].ids = append(i.buckets[flatId].ids, id)
	i.buckets[flatId].codes = append(i.buckets[flatId].codes, i.bq.Encode(vector)...)
	i.locks.Unlock(flatId)

	var buf *bytes.Buffer = new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, vector)
	toWrite := buf.Bytes()

	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	i.store.Bucket("vectors").Put(idBytes, toWrite)
}

// +reading 200 buckets * 50 sketches * 24*8 bytes = 2MB ==>> 100M -> 200MB per query
// keeping 15% of sketches 30MB ==>> 100M -> 3GB
func (i *FlatPQ) SearchByVector(ctx context.Context, searchVec []float32, k int) ([]uint64, []int, error) {
	heap := priorityqueue.NewMax[byte](i.probing)
	distancer := i.pq.NewDistancer(searchVec)

	for id := uint64(0); id < uint64(len(i.buckets)); id++ {
		i.locks.RLock(id)
		bucket := i.buckets[id]
		i.locks.RUnlock(id)
		if bucket == nil {
			continue
		}
		d, _ := distancer.Distance(bucket.code)

		if heap.Len() == i.probing {
			if d >= heap.Top().Dist {
				continue
			}
			heap.Pop()
		}
		heap.Insert(id, d)

	}

	heap_ids := priorityqueue.NewMax[byte](targetProbe)
	searchVecCode := i.bq.Encode(searchVec)
	bids := make([]uint64, 0, heap.Len())
	for heap.Len() > 0 {
		bids = append(bids, heap.Pop().ID)
	}

	ivfBucket := i.store.Bucket("ivf")

	logger := logrus.New()
	eg := enterrors.NewErrorGroupWrapper(logger)
	for workerID := 0; workerID < rescoreConcurrency; workerID++ {
		workerID := workerID

		eg.Go(func() error {
			for idPos := workerID; idPos < len(bids); idPos += rescoreConcurrency {
				bid := bids[idPos]
				i.locks.RLock(bid)
				bucket := i.buckets[bid]
				i.locks.RUnlock(bid)
				read := bucket.load(ivfBucket, i.sketchSize)
				if read > 0 {
					i.Lock()
					i.bytesRead += read
					i.Unlock()
				}
			}
			return nil
		}, logger)
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	for _, bid := range bids {
		i.locks.RLock(bid)
		bucket := i.buckets[bid]
		i.locks.RUnlock(bid)
		defer bucket.release()
		for j, id := range bucket.ids {
			d, _ := i.bq.DistanceBetweenCompressedVectors(searchVecCode, bucket.codes[j*i.sketchSize:(j+1)*i.sketchSize])
			if heap_ids.Len() == targetProbe {
				if heap_ids.Top().Dist < d {
					continue
				}
				heap_ids.Pop()
			}
			heap_ids.Insert(id, d)

		}
	}

	ids := make([]uint64, heap_ids.Len())
	j := heap_ids.Len() - 1
	for heap_ids.Len() > 0 {
		ids[j] = heap_ids.Pop().ID
		j--
	}

	heap.Reset()
	mu := sync.Mutex{} // protect res
	addID := func(id uint64, dist float32) {
		mu.Lock()
		defer mu.Unlock()

		heap.Insert(id, dist)
		if heap.Len() > k {
			heap.Pop()
		}
	}
	eg = enterrors.NewErrorGroupWrapper(logger)
	for workerID := 0; workerID < rescoreConcurrency; workerID++ {
		workerID := workerID

		eg.Go(func() error {
			for idPos := workerID; idPos < len(ids); idPos += rescoreConcurrency {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("rescore: %w", err)
				}

				id := ids[idPos]
				dist, err := i.distanceToVector(searchVec, id)
				if err == nil {
					addID(id, dist)
				}
			}
			return nil
		}, logger)
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}

	ids = make([]uint64, heap.Len())
	j = heap.Len() - 1
	for heap.Len() > 0 {
		ids[j] = heap.Pop().ID
		j--
	}
	return ids, nil, nil
}

func (i *FlatPQ) idFromCode(code []byte) uint64 {
	id := uint64(0)
	for j := 0; j < i.segments; j++ {
		id *= uint64(i.centroids)
		id += uint64(code[j])
	}
	return id
}

func (i *FlatPQ) Store() {
	ivfBucket := i.store.Bucket("ivf")
	for _, bucket := range i.buckets {
		bucket.store(ivfBucket)
	}
}

func (i *FlatPQ) MemoryInUse() int {
	total := int(reflect.TypeOf(i).Size())
	for _, bucket := range i.buckets {
		total += bucket.memInUse()
	}
	return total
}

func (i *FlatPQ) distanceToVector(searchVec []float32, id uint64) (float32, error) {
	idBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(idBytes, id)
	buf, _ := i.store.Bucket("vectors").Get(idBytes)
	vector := make([]float32, 1536)
	binary.Read(bytes.NewReader(buf), binary.LittleEndian, &vector)
	return i.distancer.SingleDist(searchVec, vector)
}

func (i *FlatPQ) BytesRead() int {
	return i.bytesRead
}
