//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hfresh

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

const (
	quantizationKey = "quantization"
	dimensionsKey   = "dimensions"
)

// MetadataStore is a persistent store for metadata.
type MetadataStore struct {
	bucket *lsmkv.Bucket
}

func NewMetadataStore(bucket *lsmkv.Bucket) *MetadataStore {
	return &MetadataStore{
		bucket: bucket,
	}
}

func (m *MetadataStore) key(suffix string) []byte {
	buf := make([]byte, 1+len(suffix))
	buf[0] = metadataBucketPrefix
	copy(buf[1:], suffix)
	return buf
}

func (m *MetadataStore) SetDimensions(dimensions uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, dimensions)
	return m.bucket.Put(m.key(dimensionsKey), buf)
}

func (m *MetadataStore) GetDimensions() (uint32, error) {
	data, err := m.bucket.Get(m.key(dimensionsKey))
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

func (m *MetadataStore) SetQuantizationData(data *QuantizationData) error {
	serialized, err := msgpack.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "marshal quantization data")
	}

	return m.bucket.Put(m.key(quantizationKey), serialized)
}

func (m *MetadataStore) GetQuantizationData() (*QuantizationData, error) {
	data, err := m.bucket.Get(m.key(quantizationKey))
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

type RQ8Data struct {
	InputDim  uint32                      `msgpack:"input_dim"`
	OutputDim uint32                      `msgpack:"output_dim"`
	Rounds    uint32                      `msgpack:"rounds"`
	Swaps     [][]compressionhelpers.Swap `msgpack:"swaps"`
	Signs     [][]float32                 `msgpack:"signs"`
}

func (h *HFresh) restoreMetadata() error {
	dims, err := h.Metadata.GetDimensions()
	if err != nil || dims == 0 {
		return err
	}
	h.initDimensionsOnce.Do(func() {
		atomic.StoreUint32(&h.dims, dims)
		h.setMaxPostingSize()

		quantization, err := h.Metadata.GetQuantizationData()
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

	return h.Metadata.SetQuantizationData(&QuantizationData{
		RQ: h.quantizer.Data(),
	})
}

// restoreQuantizationData restores RQ quantizer from msgpack data
func (h *HFresh) restoreQuantizationData(rqData *compressionhelpers.RQData) error {
	// Restore the RQ quantizer
	rq, err := compressionhelpers.RestoreRotationalQuantizer(
		int(rqData.InputDim),
		int(rqData.Bits),
		int(rqData.Rotation.OutputDim),
		int(rqData.Rotation.Rounds),
		rqData.Rotation.Swaps,
		rqData.Rotation.Signs,
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
