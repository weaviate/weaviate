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

package hfresh

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

const (
	quantizationKey    = "quantization"
	dimensionsKey      = "dimensions"
	postingSequenceKey = "posting_seq"
	muveraKey          = "muvera"
	singlePostingKey   = "single_posting"
	centroidsBuiltKey  = "centroids_built"
)

// The shared bucket is used to store various metadata. It is used by multiple stores
// and the data is namespaced by using different prefixes for the keys.
// The shared bucket format itself is versioned, so that we can make non-compatible changes in the future if needed.
const (
	sharedBucketVersionV1 = 1
)

// These constants define the prefixes used in the
// lsmkv bucket to namespace different types of data.
var (
	indexMetadataBucketPrefix = []byte{sharedBucketVersionV1, 0}
	versionMapBucketPrefix    = []byte{sharedBucketVersionV1, 1}
	// postingMapBucketPrefixV1 is migrated to postingMapBucketPrefixV2 by migratePostingMapV1ToV2.
	postingMapBucketPrefixV1   = []byte{sharedBucketVersionV1, 2}
	postingVersionBucketPrefix = []byte{sharedBucketVersionV1, 3}
	// reassignBucketKey is a legacy shared-bucket key used by the old persisted
	// reassign deduplicator. It is intentionally reserved forever so no future
	// metadata can accidentally read stale reassign blobs as a different format.
	// New code must not write to this key.
	reassignBucketKey = []byte{sharedBucketVersionV1, 4, 0}
	// postingMapBucketPrefixV2 stores posting IDs without vector version bytes.
	postingMapBucketPrefixV2 = []byte{sharedBucketVersionV1, 5}
	// postingSizesBucketPrefix was added with postingMapBucketPrefixV2.
	postingSizesBucketPrefix = []byte{sharedBucketVersionV1, 6}
)

// NewSharedBucket creates a shared lsmkv bucket for the HFresh index.
// This bucket is used to store metadata in namespaced regions of the bucket.
func NewSharedBucket(store *lsmkv.Store, indexID string, cfg StoreConfig) (bucketRef, error) {
	bName := sharedBucketName(indexID)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return bucketRef{}, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	ref := newBucketRef(store, bName)
	bucket, release, err := ref.acquire()
	if err != nil {
		return bucketRef{}, err
	}
	defer release()

	if err := cleanupLegacyReassignBucket(bucket); err != nil {
		return bucketRef{}, err
	}

	return ref, nil
}

func sharedBucketName(id string) string {
	return helpers.HFreshSharedBucketName(id)
}

func cleanupLegacyReassignBucket(bucket *lsmkv.Bucket) error {
	exists, err := bucket.Exists(reassignBucketKey)
	if err != nil {
		return errors.Wrap(err, "check legacy reassign bucket key")
	}
	if !exists {
		return nil
	}

	err = bucket.Delete(reassignBucketKey)
	if err != nil {
		return errors.Wrap(err, "delete legacy reassign bucket key")
	}
	return nil
}

// IndexMetadataStore manages metadata for the index, such as dimensions and quantization data.
type IndexMetadataStore struct {
	bucket bucketRef
}

func NewIndexMetadataStore(bucket bucketRef) *IndexMetadataStore {
	return &IndexMetadataStore{
		bucket: bucket,
	}
}

func (i *IndexMetadataStore) key(suffix string) []byte {
	buf := make([]byte, len(indexMetadataBucketPrefix)+len(suffix))
	copy(buf, indexMetadataBucketPrefix)
	copy(buf[len(indexMetadataBucketPrefix):], suffix)
	return buf
}

func (i *IndexMetadataStore) SetDimensions(dimensions uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, dimensions)
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(i.key(dimensionsKey), buf)
}

func (i *IndexMetadataStore) GetDimensions() (uint32, error) {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return 0, err
	}
	defer release()

	data, err := bucket.Get(i.key(dimensionsKey))
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil // Not set yet
	}
	if len(data) != 4 {
		return 0, fmt.Errorf("invalid dimensions data length: %d", len(data))
	}
	dimensions := binary.LittleEndian.Uint32(data)
	return dimensions, nil
}

func (i *IndexMetadataStore) SetQuantizationData(data *QuantizationData) error {
	serialized, err := msgpack.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "marshal quantization data")
	}

	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(i.key(quantizationKey), serialized)
}

func (i *IndexMetadataStore) GetQuantizationData() (*QuantizationData, error) {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	data, err := bucket.Get(i.key(quantizationKey))
	if err != nil {
		return nil, errors.Wrap(err, "get quantization data")
	}
	if data == nil {
		return nil, nil // Not set yet
	}

	var qData QuantizationData
	err = msgpack.Unmarshal(data, &qData)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal quantization data")
	}

	return &qData, nil
}

type QuantizationData struct {
	RQ compression.RQData `msgpack:"rq"`
}

// SetSinglePosting records the posting that holds every vector while no
// centroid index exists: its id followed by its float32 centroid.
func (i *IndexMetadataStore) SetSinglePosting(id uint64, centroid []float32) error {
	buf := make([]byte, 8+4*len(centroid))
	binary.LittleEndian.PutUint64(buf, id)
	for j, f := range centroid {
		binary.LittleEndian.PutUint32(buf[8+4*j:], math.Float32bits(f))
	}

	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(i.key(singlePostingKey), buf)
}

// GetSinglePosting returns id 0 when no single posting was recorded.
func (i *IndexMetadataStore) GetSinglePosting() (uint64, []float32, error) {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return 0, nil, err
	}
	defer release()

	data, err := bucket.Get(i.key(singlePostingKey))
	if err != nil {
		return 0, nil, errors.Wrap(err, "get single posting")
	}
	if data == nil {
		return 0, nil, nil
	}
	if len(data) < 8 || (len(data)-8)%4 != 0 {
		return 0, nil, fmt.Errorf("invalid single posting data length: %d", len(data))
	}

	id := binary.LittleEndian.Uint64(data)
	centroid := make([]float32, (len(data)-8)/4)
	for j := range centroid {
		centroid[j] = math.Float32frombits(binary.LittleEndian.Uint32(data[8+4*j:]))
	}
	return id, centroid, nil
}

func (i *IndexMetadataStore) SetCentroidsBuilt() error {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(i.key(centroidsBuiltKey), []byte{1})
}

func (i *IndexMetadataStore) CentroidsBuilt() (bool, error) {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return false, err
	}
	defer release()

	data, err := bucket.Get(i.key(centroidsBuiltKey))
	if err != nil {
		return false, errors.Wrap(err, "get centroids built flag")
	}
	return len(data) == 1 && data[0] == 1, nil
}

func (i *IndexMetadataStore) SetMuveraData(data *compression.MuveraData) error {
	serialized, err := msgpack.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "marshal muvera data")
	}
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(i.key(muveraKey), serialized)
}

func (i *IndexMetadataStore) GetMuveraData() (*compression.MuveraData, error) {
	bucket, release, err := i.bucket.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	data, err := bucket.Get(i.key(muveraKey))
	if err != nil {
		return nil, errors.Wrap(err, "get muvera data")
	}
	if data == nil {
		return nil, nil
	}

	var muveraData compression.MuveraData
	if err = msgpack.Unmarshal(data, &muveraData); err != nil {
		return nil, errors.Wrap(err, "unmarshal muvera data")
	}
	return &muveraData, nil
}

func (h *HFresh) restoreMetadata() error {
	dims, err := h.IndexMetadata.GetDimensions()
	if err != nil || dims == 0 {
		return err
	}

	if err := h.restoreDimensions(dims); err != nil {
		return err
	}

	if h.muvera.Load() {
		muveraData, err := h.IndexMetadata.GetMuveraData()
		if err != nil {
			return err
		}
		if muveraData != nil {
			h.trackMuveraOnce.Do(func() {
				h.muveraEncoder.LoadMuveraConfig(*muveraData)
			})
		}
	}

	if err := h.restoreCentroids(); err != nil {
		return err
	}

	return h.restoreMetrics()
}

func (h *HFresh) restoreCentroidGraph() error {
	postingMap := h.PostingMap()
	if postingMap == nil {
		postingMap = NewPostingMap(h.sharedBucket)
	}
	err := migratePostingMapV1ToV2(h.ctx, postingMap.bucket.bucket, h.logger)
	if err != nil {
		return err
	}
	err = postingMap.Restore(h.ctx)
	if err != nil {
		return err
	}
	// after the migration, which also writes posting sizes
	err = h.PostingSizes.Restore(h.ctx)
	if err != nil {
		return err
	}
	h.postingMap.Store(postingMap)

	return h.Centroids.Build()
}

func (h *HFresh) centroidCommitLogExists() bool {
	hnswCfg := h.config.Centroids.HNSWConfig
	_, err := os.Stat(filepath.Join(hnswCfg.RootPath, hnswCfg.ID+".hnsw.commitlog.d"))
	return err == nil
}

// recoverSinglePosting rebuilds the single posting record for an index
// that predates it. The initial posting always gets id 1 and its centroid
// is one of its own vectors, so any entry's full-precision vector serves.
func (h *HFresh) recoverSinglePosting() (uint64, []float32, error) {
	const initialPostingID = 1

	posting, err := h.PostingStore.Get(h.ctx, initialPostingID)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "read posting %d", initialPostingID)
	}
	if len(posting) == 0 {
		return 0, nil, nil
	}

	var (
		view  common.BucketView
		slice *common.VectorSlice
	)
	if !h.muvera.Load() {
		objectsView, releaseView, err := h.objectsBucketView()
		if err != nil {
			return 0, nil, err
		}
		defer releaseView()
		view = objectsView

		slice = h.tempVectors.Get(int(atomic.LoadUint32(&h.dims)))
		defer h.tempVectors.Put(slice)
	}

	centroid, err := h.clusteringVector(h.ctx, posting[0].ID(), slice, view)
	if err != nil {
		return 0, nil, errors.Wrapf(err, "fetch centroid vector for posting %d", initialPostingID)
	}

	err = h.IndexMetadata.SetSinglePosting(initialPostingID, centroid)
	if err != nil {
		return 0, nil, errors.Wrap(err, "persist recovered single posting")
	}
	h.logger.WithField("postingID", initialPostingID).
		Warnf("recovered single posting record with %d vectors", len(posting))

	return initialPostingID, centroid, nil
}

// restoreCentroids brings the centroid index back to its pre-restart state:
// either the graph is reloaded from its commit log, or the single posting's
// id and centroid are restored and its size counter, which only lives in
// memory before the first split, is recounted from the posting on disk.
func (h *HFresh) restoreCentroids() error {
	built := h.Centroids.Built()
	if !built {
		flag, err := h.IndexMetadata.CentroidsBuilt()
		if err != nil {
			return err
		}
		built = flag
	}

	var (
		id       uint64
		centroid []float32
		err      error
	)
	if !built {
		id, centroid, err = h.IndexMetadata.GetSinglePosting()
		if err != nil {
			return err
		}
		// written before the single posting was recorded: a commit log means
		// the graph was built
		built = id == 0 && h.centroidCommitLogExists()
	}
	if built {
		return h.restoreCentroidGraph()
	}

	err = h.PostingSizes.Restore(h.ctx)
	if err != nil {
		return err
	}
	if id == 0 {
		id, centroid, err = h.recoverSinglePosting()
		if err != nil || id == 0 {
			return err
		}
	}

	h.Centroids.restoreSingle(id, centroid)

	// The background version warmup fills defaults from the posting map,
	// which is empty here. Persisted versions go in first: memory wins, so a
	// default installed before a persisted delete would resurrect the vector.
	_, err = h.VersionMap.Warmup(h.ctx)
	if err != nil {
		return errors.Wrap(err, "warm up versions of the single posting")
	}

	posting, err := h.PostingStore.Get(h.ctx, id)
	if err != nil {
		return errors.Wrapf(err, "read single posting %d", id)
	}
	for _, v := range posting {
		h.PostingSizes.FastIncrement(id)
		h.VersionMap.EnsureDefault(v.ID())
	}
	return nil
}

// restoreDimensions restores dimension-dependent components from persisted
// metadata. If quantization data is missing (e.g. crash before it was
// persisted), the quantizer is re-created from scratch.
func (h *HFresh) restoreDimensions(dims uint32) error {
	h.initMu.Lock()
	defer h.initMu.Unlock()

	if h.initDone {
		return nil
	}

	if err := h.setMaxPostingSize(dims); err != nil {
		return err
	}

	quantization, err := h.IndexMetadata.GetQuantizationData()
	if err != nil {
		return err
	}

	if quantization != nil {
		if err := h.restoreQuantizationData(&quantization.RQ); err != nil {
			return err
		}
	} else {
		// Dimensions were persisted but quantization data was not (e.g. crash
		// between persisting dimensions and quantization data). Re-create the
		// quantizer from scratch.
		quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(int(dims), 42, h.config.DistanceProvider)
		if err != nil {
			return errors.Wrap(err, "could not create quantizer")
		}
		h.quantizer = quantizer
		h.distancer = NewDistancer(h.quantizer, h.config.DistanceProvider)
		if err := h.persistQuantizationData(); err != nil {
			return errors.Wrap(err, "could not persist RQ data")
		}
	}

	h.initDone = true
	atomic.StoreUint32(&h.dims, dims)
	return nil
}

func (h *HFresh) persistQuantizationData() error {
	if h.quantizer == nil {
		return nil
	}

	return h.IndexMetadata.SetQuantizationData(&QuantizationData{
		RQ: h.quantizer.Data(),
	})
}

func (h *HFresh) restoreMetrics() error {
	splitCount := h.taskQueue.splitQueue.Size()
	mergeCount := h.taskQueue.mergeQueue.Size()
	reassignCount := h.taskQueue.reassignQueue.Size()
	analyzeCount := h.taskQueue.analyzeQueue.Size()

	h.metrics.SetPendingSplitTasks(splitCount)
	h.metrics.SetPendingMergeTasks(mergeCount)
	h.metrics.SetPendingReassignTasks(reassignCount)
	h.metrics.SetPendingAnalyzeTasks(analyzeCount)

	postingsCount := int(h.PostingSizes.Count())
	h.Centroids.counter.Store(int32(postingsCount))
	h.metrics.SetPostings(postingsCount)

	return nil
}

// restoreQuantizationData restores RQ quantizer from msgpack data
func (h *HFresh) restoreQuantizationData(rqData *compression.RQData) error {
	// Restore the RQ quantizer
	rq, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
		int(rqData.InputDim),
		int(rqData.Rotation.OutputDim),
		int(rqData.Rotation.Rounds),
		rqData.Rotation.Swaps,
		rqData.Rotation.Signs,
		rqData.Rounding,
		h.config.DistanceProvider,
	)
	if err != nil {
		return errors.Wrap(err, "restore rotational quantizer from msgpack")
	}

	h.quantizer = rq
	h.distancer = NewDistancer(rq, h.config.DistanceProvider)

	return nil
}

// BucketStore is a SequenceStore implementation that uses the LSM store as the backend.
type BucketStore struct {
	bucket bucketRef
	key    []byte
}

func NewBucketStore(bucket bucketRef) *BucketStore {
	return &BucketStore{
		bucket: bucket,
		key:    []byte(postingSequenceKey),
	}
}

func (s *BucketStore) Store(upperBound uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], upperBound)

	bucket, release, err := s.bucket.acquire()
	if err != nil {
		return err
	}
	defer release()

	return bucket.Put(s.key, buf[:])
}

func (s *BucketStore) Load() (uint64, error) {
	bucket, release, err := s.bucket.acquire()
	if err != nil {
		return 0, err
	}
	defer release()

	v, err := bucket.Get(s.key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil // Not set yet
	}

	return binary.LittleEndian.Uint64(v), nil
}
