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
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

const (
	quantizationKey    = "quantization"
	dimensionsKey      = "dimensions"
	postingSequenceKey = "posting_seq"
)

// These constants define the prefixes used in the
// lsmkv bucket to namespace different types of data.
const (
	versionMapBucketPrefix      = 'v'
	metadataBucketPrefix        = 'm'
	postingMetadataBucketPrefix = 'p'
	reassignBucketKey           = "pending_reassignments"
)

// NewSharedBucket creates a shared lsmkv bucket for the HFresh index.
// This bucket is used to store metadata in namespaced regions of the bucket.
func NewSharedBucket(store *lsmkv.Store, indexID string, cfg StoreConfig) (*lsmkv.Bucket, error) {
	bName := sharedBucketName(indexID)
	err := store.CreateOrLoadBucket(context.Background(),
		bName,
		cfg.MakeBucketOptions(lsmkv.StrategyReplace, lsmkv.WithForceCompaction(true))...,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create or load bucket %s", bName)
	}

	return store.Bucket(bName), nil
}

func sharedBucketName(id string) string {
	return fmt.Sprintf("hfresh_shared_%s", id)
}

// IndexMetadataStore manages metadata for the index, such as dimensions and quantization data.
type IndexMetadataStore struct {
	bucket *lsmkv.Bucket
}

func NewIndexMetadataStore(bucket *lsmkv.Bucket) *IndexMetadataStore {
	return &IndexMetadataStore{
		bucket: bucket,
	}
}

func (i *IndexMetadataStore) key(suffix string) []byte {
	buf := make([]byte, 1+len(suffix))
	buf[0] = metadataBucketPrefix
	copy(buf[1:], suffix)
	return buf
}

func (i *IndexMetadataStore) SetDimensions(dimensions uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, dimensions)
	return i.bucket.Put(i.key(dimensionsKey), buf)
}

func (i *IndexMetadataStore) GetDimensions() (uint32, error) {
	data, err := i.bucket.Get(i.key(dimensionsKey))
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

	return i.bucket.Put(i.key(quantizationKey), serialized)
}

func (i *IndexMetadataStore) GetQuantizationData() (*QuantizationData, error) {
	data, err := i.bucket.Get(i.key(quantizationKey))
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
	RQ compressionhelpers.RQData `msgpack:"rq"`
}

func (h *HFresh) restoreMetadata() error {
	dims, err := h.IndexMetadata.GetDimensions()
	if err != nil || dims == 0 {
		return err
	}
	h.initDimensionsOnce.Do(func() {
		atomic.StoreUint32(&h.dims, dims)
		err = h.setMaxPostingSize()
		if err != nil {
			return
		}

		var quantization *QuantizationData
		quantization, err = h.IndexMetadata.GetQuantizationData()
		if err != nil {
			return
		}

		if quantization != nil {
			err = h.restoreQuantizationData(&quantization.RQ)
		}
	})

	return err
}

func (h *HFresh) persistQuantizationData() error {
	if h.quantizer == nil {
		return nil
	}

	return h.IndexMetadata.SetQuantizationData(&QuantizationData{
		RQ: h.quantizer.Data(),
	})
}

// restoreQuantizationData restores RQ quantizer from msgpack data
func (h *HFresh) restoreQuantizationData(rqData *compressionhelpers.RQData) error {
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
	h.Centroids.SetQuantizer(rq)
	h.distancer = &Distancer{
		quantizer: rq,
		distancer: h.config.DistanceProvider,
	}

	return nil
}

// BucketStore is a SequenceStore implementation that uses the LSM store as the backend.
type BucketStore struct {
	bucket *lsmkv.Bucket
	key    []byte
}

func NewBucketStore(bucket *lsmkv.Bucket) *BucketStore {
	return &BucketStore{
		bucket: bucket,
		key:    []byte(postingSequenceKey),
	}
}

func (s *BucketStore) Store(upperBound uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], upperBound)

	return s.bucket.Put(s.key, buf[:])
}

func (s *BucketStore) Load() (uint64, error) {
	v, err := s.bucket.Get(s.key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return 0, nil // Not set yet
	}

	return binary.LittleEndian.Uint64(v), nil
}
