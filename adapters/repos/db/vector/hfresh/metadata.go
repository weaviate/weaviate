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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	bolt "go.etcd.io/bbolt"
)

const (
	metadataPrefix       = "meta"
	vectorMetadataBucket = "vector"
	quantizationKey      = "quantization"
)

type RQDataContainer struct {
	Data interface{} `msgpack:"data"` // The actual RQ data
}

// RQ1Data represents RQ1 (Binary Rotational Quantization) data
type RQ1Data struct {
	InputDim  uint32                      `msgpack:"input_dim"`
	OutputDim uint32                      `msgpack:"output_dim"`
	Rounds    uint32                      `msgpack:"rounds"`
	Swaps     [][]compressionhelpers.Swap `msgpack:"swaps"`
	Signs     [][]float32                 `msgpack:"signs"`
	Rounding  []float32                   `msgpack:"rounding"`
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
	err := h.openMetadata()
	if err != nil {
		return err
	}
	defer h.closeMetadata()

	err = h.metadata.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(vectorMetadataBucket))
		if err != nil {
			return errors.Wrap(err, "create bucket")
		}
		if b == nil {
			return errors.New("failed to create or get bucket")
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "init metadata bucket")
	}

	// Restore RQ data if available
	if err := h.restoreRQData(); err != nil {
		h.logger.Warnf("HFresh index unable to restore RQ data: %v", err)
	}

	if err := h.initDimensions(); err != nil {
		h.logger.Warnf("HFresh index unable to restore RQ data: %v", err)
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

func (h *HFresh) setVectorSize(vectorSize int32) error {
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
		binary.LittleEndian.PutUint32(buf, uint32(vectorSize))
		return b.Put([]byte("vector_size"), buf)
	})
	if err != nil {
		return errors.Wrap(err, "set vector size")
	}

	return nil
}

func (h *HFresh) restoreVectorSize() error {
	err := h.openMetadata()
	if err != nil {
		return err
	}
	defer h.closeMetadata()

	var vectorSize int32
	err = h.metadata.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil
		}
		v := b.Get([]byte("vector_size"))
		if v == nil {
			return nil
		}
		vectorSize = int32(binary.LittleEndian.Uint32(v))
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "restore vector size")
	}

	if vectorSize > 0 {
		atomic.StoreInt32(&h.vectorSize, vectorSize)
		h.PostingStore.vectorSize.Store(vectorSize)
	}

	return nil
}

// RQ data persistence and restoration functions

func (h *HFresh) persistRQData() error {
	if h.quantizer == nil {
		return nil
	}

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

		// Create RQ data container with metadata
		container := &RQDataContainer{}

		rq1Data, err := h.serializeRQ1Data()
		if err != nil {
			return errors.Wrap(err, "serialize RQ8 data")
		}
		container.Data = rq1Data

		// Serialize to msgpack
		data, err := msgpack.Marshal(container)
		if err != nil {
			return errors.Wrap(err, "marshal RQ data container")
		}

		// Store the serialized data
		return b.Put([]byte(quantizationKey), data)
	})
	if err != nil {
		return errors.Wrap(err, "persist RQ data")
	}

	return nil
}

// serializeRQ1Data extracts RQ1 data from the quantizer and converts it to msgpack format
func (index *HFresh) serializeRQ1Data() (*RQ1Data, error) {
	// Use a custom commit logger to capture the BRQ data
	captureLogger := &dataCaptureLogger{}
	index.quantizer.PersistCompression(captureLogger)

	if captureLogger.brqData == nil {
		return nil, errors.New("no BRQ data captured from quantizer")
	}

	index.logger.Debugf("Captured BRQ data: InputDim=%d, OutputDim=%d, Rounds=%d, Swaps=%d, Signs=%d, Rounding=%d",
		captureLogger.brqData.InputDim,
		captureLogger.brqData.Rotation.OutputDim,
		captureLogger.brqData.Rotation.Rounds,
		len(captureLogger.brqData.Rotation.Swaps),
		len(captureLogger.brqData.Rotation.Signs),
		len(captureLogger.brqData.Rounding))

	return &RQ1Data{
		InputDim:  captureLogger.brqData.InputDim,
		OutputDim: captureLogger.brqData.Rotation.OutputDim,
		Rounds:    captureLogger.brqData.Rotation.Rounds,
		Swaps:     captureLogger.brqData.Rotation.Swaps,
		Signs:     captureLogger.brqData.Rotation.Signs,
		Rounding:  captureLogger.brqData.Rounding,
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

	var rq1Data RQ1Data
	if err := msgpack.Unmarshal(dataBytes, &rq1Data); err != nil {
		return errors.Wrap(err, "unmarshal RQ8Data")
	}

	h.logger.Warnf("Successfully deserialized RQ8Data: InputDim=%d",
		rq1Data.InputDim)
	return h.restoreRQ1FromMsgpack(&rq1Data)
}

// restoreRQ8FromMsgpack restores RQ1 quantizer from msgpack data
func (h *HFresh) restoreRQ1FromMsgpack(rq1Data *RQ1Data) error {
	// Restore the RQ1 quantizer
	rq, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
		int(rq1Data.InputDim),
		int(rq1Data.OutputDim),
		int(rq1Data.Rounds),
		rq1Data.Swaps,
		rq1Data.Signs,
		rq1Data.Rounding,
		h.config.DistanceProvider,
	)
	if err != nil {
		return errors.Wrap(err, "restore binary rotational quantizer from msgpack")
	}

	h.quantizer = rq
	h.Centroids.SetQuantizer(rq)
	h.distancer = &Distancer{
		quantizer: rq,
		distancer: h.config.DistanceProvider,
	}

	// Restore vector size if available
	if err := h.restoreVectorSize(); err != nil {
		h.logger.Warnf("HFresh index unable to restore vector size: %v", err)
	}

	h.PostingStore.Init(h.vectorSize)
	return nil
}
