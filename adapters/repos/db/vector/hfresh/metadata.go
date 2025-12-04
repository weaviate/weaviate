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
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	bolt "go.etcd.io/bbolt"
)

const (
	metadataPrefix       = "meta"
	vectorMetadataBucket = "vector"
	quantizationKey      = "quantization"
	dimensionsKey        = "dimensions"
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

func (h *HFresh) getMetadataFile() string {
	if h.config.TargetVector != "" {
		cleanTarget := filepath.Clean(h.config.TargetVector)
		cleanTarget = filepath.Base(cleanTarget)
		return fmt.Sprintf("%s_%s.db", metadataPrefix, cleanTarget)
	}
	return fmt.Sprintf("%s.db", metadataPrefix)
}

func (h *HFresh) closeMetadata() {
	h.metadataLock.Lock()
	defer h.metadataLock.Unlock()

	if h.metadata != nil {
		h.metadata.Close()
		h.metadata = nil
	}
}

func (h *HFresh) openMetadata() error {
	h.metadataLock.Lock()
	defer h.metadataLock.Unlock()

	if h.metadata != nil {
		return nil // Already open
	}

	path := filepath.Join(filepath.Dir(h.config.RootPath), h.getMetadataFile())
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open %q", path)
	}

	h.metadata = db
	return nil
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

func (h *HFresh) initDimensions() error {
	dims, err := h.fetchDimensions()
	if err != nil {
		return errors.Wrap(err, "HFresh index unable to fetch dimensions")
	}

	if dims > 0 {
		atomic.StoreInt32(&h.dims, dims)
	}
	return nil
}

func (h *HFresh) fetchDimensions() (int32, error) {
	if h.metadata == nil {
		return 0, nil
	}

	var dimensions int32 = 0
	err := h.metadata.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte("dimensions"))
		if v == nil {
			return nil
		}
		dimensions = int32(binary.LittleEndian.Uint32(v))
		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, "fetch dimensions")
	}

	return dimensions, nil
}

func (h *HFresh) setDimensions(dimensions int32) error {
	err := h.openMetadata()
	if err != nil {
		return err
	}
	defer h.closeMetadata()

	err = h.metadata.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return errors.New("failed to get bucket")
		}
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(dimensions))
		return b.Put([]byte("dimensions"), buf)
	})
	if err != nil {
		return errors.Wrap(err, "set dimensions")
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

func (h *HFresh) restoreRQData() error {
	var container *RQDataContainer
	var data []byte

	err := h.metadata.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil // No metadata yet
		}

		// Check if RQ data exists
		data = b.Get([]byte(quantizationKey))
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "restore RQ data")
	}
	// No data found in bucket
	if data == nil {
		return nil
	}

	// Try to deserialize as msgpack
	err = msgpack.Unmarshal(data, &container)
	if err != nil {
		return errors.New("failed to deserialize RQ data - unknown format")
	}

	// Handle the Data field manually since msgpack deserializes interface{} as map[string]interface{}
	return h.handleDeserializedData(container)
}

// handleDeserializedData manually handles the Data field deserialization
func (h *HFresh) handleDeserializedData(container *RQDataContainer) error {
	// Deserialize the Data field as RQ8Data
	dataBytes, err := msgpack.Marshal(container.Data)
	if err != nil {
		return errors.Wrap(err, "marshal container data for RQ8")
	}

	var rq8Data RQ8Data
	if err := msgpack.Unmarshal(dataBytes, &rq8Data); err != nil {
		return errors.Wrap(err, "unmarshal RQ8Data")
	}

	h.logger.Warnf("Successfully deserialized RQ8Data: InputDim=%d",
		rq8Data.InputDim)
	return h.restoreRQ8FromMsgpack(&rq8Data)
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
