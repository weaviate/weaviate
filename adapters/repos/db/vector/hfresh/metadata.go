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
	RQ8 *RQ8Data `msgpack:"rq8"`
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
	if err != nil {
		return err
	}
	if dims > 0 {
		atomic.StoreUint32(&h.dims, dims)
		h.setMaxPostingSize()
	}

	quantization, err := h.Metadata.GetQuantizationData()
	if err != nil {
		return err
	}

	if quantization != nil && quantization.RQ8 != nil {
		return h.restoreRQ8FromMsgpack(quantization.RQ8)
	}

	return nil
}

// RQ data persistence and restoration functions

func (h *HFresh) persistRQData() error {
	if h.quantizer == nil {
		return nil
	}

	rq8Data, err := h.serializeRQ8Data()
	if err != nil {
		return errors.Wrap(err, "serialize RQ8 data")
	}

	return h.Metadata.SetQuantizationData(&QuantizationData{
		RQ8: rq8Data,
	})
}

// serializeRQ8Data extracts RQ8 data from the quantizer and converts it to msgpack format
func (h *HFresh) serializeRQ8Data() (*RQ8Data, error) {
	// Use a custom commit logger to capture the RQ data
	captureLogger := &dataCaptureLogger{}
	h.quantizer.PersistCompression(captureLogger)

	if captureLogger.rqData == nil {
		return nil, errors.New("no RQ data captured from quantizer")
	}

	h.logger.Debugf("Captured RQ data: InputDim=%d, Bits=%d, OutputDim=%d, Rounds=%d, Swaps=%d, Signs=%d",
		captureLogger.rqData.InputDim,
		captureLogger.rqData.Bits,
		captureLogger.rqData.Rotation.OutputDim,
		captureLogger.rqData.Rotation.Rounds,
		len(captureLogger.rqData.Rotation.Swaps),
		len(captureLogger.rqData.Rotation.Signs))

	return &RQ8Data{
		InputDim:  captureLogger.rqData.InputDim,
		OutputDim: captureLogger.rqData.Rotation.OutputDim,
		Rounds:    captureLogger.rqData.Rotation.Rounds,
		Swaps:     captureLogger.rqData.Rotation.Swaps,
		Signs:     captureLogger.rqData.Rotation.Signs,
	}, nil
}

// dataCaptureLogger captures RQ data from quantizer PersistCompression calls
type dataCaptureLogger struct {
	rqData  *compressionhelpers.RQData
	brqData *compressionhelpers.BRQData
}

func (d *dataCaptureLogger) AddRQCompression(data compressionhelpers.RQData) error {
	d.rqData = &data
	return nil
}

func (d *dataCaptureLogger) AddBRQCompression(data compressionhelpers.BRQData) error {
	d.brqData = &data
	return nil
}

func (d *dataCaptureLogger) AddPQCompression(data compressionhelpers.PQData) error {
	return nil // Not used for flat index
}

func (d *dataCaptureLogger) AddSQCompression(data compressionhelpers.SQData) error {
	return nil // Not used for flat index
}

// restoreRQ8FromMsgpack restores RQ8 quantizer from msgpack data
func (h *HFresh) restoreRQ8FromMsgpack(rq8Data *RQ8Data) error {
	// Restore the RQ8 quantizer
	rq, err := compressionhelpers.RestoreRotationalQuantizer(
		int(rq8Data.InputDim),
		int(8),
		int(rq8Data.OutputDim),
		int(rq8Data.Rounds),
		rq8Data.Swaps,
		rq8Data.Signs,
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
