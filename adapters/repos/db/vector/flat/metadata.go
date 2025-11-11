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

package flat

import (
	"encoding/binary"
	"fmt"
	"os"
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

	// RQ data serialization version
	RQDataVersion = 1

	// Compression type strings for persistence
	CompressionTypeRQ1 = "rq1"
	CompressionTypeRQ8 = "rq8"
)

// RQDataContainer wraps RQ data with metadata for safe persistence
type RQDataContainer struct {
	Version         uint32      `msgpack:"version"`          // Serialization format version
	CompressionType string      `msgpack:"compression_type"` // Compression type for validation
	Data            interface{} `msgpack:"data"`             // The actual RQ data
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

// RQ8Data represents RQ8 (8-bit Rotational Quantization) data
type RQ8Data struct {
	InputDim  uint32                      `msgpack:"input_dim"`
	Bits      uint32                      `msgpack:"bits"`
	OutputDim uint32                      `msgpack:"output_dim"`
	Rounds    uint32                      `msgpack:"rounds"`
	Swaps     [][]compressionhelpers.Swap `msgpack:"swaps"`
	Signs     [][]float32                 `msgpack:"signs"`
}

func (index *flat) getMetadataFile() string {
	if index.targetVector != "" {
		// This may be redundant as target vector is already validated in the schema
		cleanTarget := filepath.Clean(index.targetVector)
		cleanTarget = filepath.Base(cleanTarget)
		return fmt.Sprintf("%s_%s.db", metadataPrefix, cleanTarget)
	}
	return fmt.Sprintf("%s.db", metadataPrefix)
}

func (index *flat) removeMetadataFile() error {
	path := filepath.Join(index.rootPath, index.getMetadataFile())
	index.closeMetadata()
	err := os.Remove(path)
	if err != nil {
		return errors.Wrapf(err, "remove metadata file %q", path)
	}
	return nil
}

func (index *flat) closeMetadata() {
	index.metadataLock.Lock()
	defer index.metadataLock.Unlock()

	if index.metadata != nil {
		index.metadata.Close()
		index.metadata = nil
	}
}

func (index *flat) openMetadata() error {
	index.metadataLock.Lock()
	defer index.metadataLock.Unlock()

	if index.metadata != nil {
		return nil // Already open
	}

	path := filepath.Join(index.rootPath, index.getMetadataFile())
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return errors.Wrapf(err, "open %q", path)
	}

	index.metadata = db
	return nil
}

func (index *flat) restoreMetadata() error {
	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	err = index.metadata.Update(func(tx *bolt.Tx) error {
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

	index.initDimensions()

	// Restore RQ data if available
	if err := index.restoreRQData(); err != nil {
		index.logger.Warnf("flat index unable to restore RQ data: %v", err)
	}

	return nil
}

func (index *flat) initDimensions() {
	dims, err := index.fetchDimensions()
	if err != nil {
		index.logger.Warnf("flat index unable to fetch dimensions: %v", err)
	}

	if dims == 0 {
		dims = index.calculateDimensions()
		if dims > 0 {
			// Backwards compatibility: set the dimensions in the metadata file
			err = index.setDimensions(dims)
			if err != nil {
				index.logger.Warnf("flat index unable to set dimensions: %v", err)
			}
		}
	}
	if dims > 0 {
		atomic.StoreInt32(&index.dims, dims)
	}
}

func (index *flat) fetchDimensions() (int32, error) {
	if index.metadata == nil {
		return 0, nil
	}

	var dimensions int32 = 0
	err := index.metadata.View(func(tx *bolt.Tx) error {
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

func (index *flat) calculateDimensions() int32 {
	bucket := index.store.Bucket(index.getBucketName())
	if bucket == nil {
		return 0
	}
	cursor := bucket.Cursor()
	defer cursor.Close()

	var key []byte
	var v []byte
	const maxCursorSize = 100000
	i := 0
	for key, v = cursor.First(); key != nil; key, v = cursor.Next() {
		if len(v) > 0 {
			return int32(len(v) / 4)
		}
		if i > maxCursorSize {
			break
		}
		i++
	}
	return 0
}

func (index *flat) setDimensions(dimensions int32) error {
	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	err = index.metadata.Update(func(tx *bolt.Tx) error {
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

func (index *flat) persistRQData() error {
	if index.quantizer == nil || index.compressionType == CompressionBQ {
		return nil
	}

	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	err = index.metadata.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return errors.New("failed to get bucket")
		}

		// Create RQ data container with metadata
		container := &RQDataContainer{
			Version: RQDataVersion,
		}

		// Determine compression type and serialize appropriate data
		switch index.compressionType {
		case CompressionRQ1:
			container.CompressionType = CompressionTypeRQ1
			rq1Data, err := index.serializeRQ1Data()
			if err != nil {
				return errors.Wrap(err, "serialize RQ1 data")
			}
			container.Data = rq1Data
		case CompressionRQ8:
			container.CompressionType = CompressionTypeRQ8
			rq8Data, err := index.serializeRQ8Data()
			if err != nil {
				return errors.Wrap(err, "serialize RQ8 data")
			}
			container.Data = rq8Data
		default:
			return nil // No RQ data to persist
		}

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
func (index *flat) serializeRQ1Data() (*RQ1Data, error) {
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

// serializeRQ8Data extracts RQ8 data from the quantizer and converts it to msgpack format
func (index *flat) serializeRQ8Data() (*RQ8Data, error) {
	// Use a custom commit logger to capture the RQ data
	captureLogger := &dataCaptureLogger{}
	index.quantizer.PersistCompression(captureLogger)

	if captureLogger.rqData == nil {
		return nil, errors.New("no RQ data captured from quantizer")
	}

	index.logger.Debugf("Captured RQ data: InputDim=%d, Bits=%d, OutputDim=%d, Rounds=%d, Swaps=%d, Signs=%d",
		captureLogger.rqData.InputDim,
		captureLogger.rqData.Bits,
		captureLogger.rqData.Rotation.OutputDim,
		captureLogger.rqData.Rotation.Rounds,
		len(captureLogger.rqData.Rotation.Swaps),
		len(captureLogger.rqData.Rotation.Signs))

	return &RQ8Data{
		InputDim:  captureLogger.rqData.InputDim,
		Bits:      captureLogger.rqData.Bits,
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

func (index *flat) restoreRQData() error {
	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	var container *RQDataContainer
	var data []byte

	err = index.metadata.View(func(tx *bolt.Tx) error {
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

	index.logger.Debugf("Deserialized RQ container: Version=%d, CompressionType=%s, Data type=%T",
		container.Version, container.CompressionType, container.Data)

	// Handle the Data field manually since msgpack deserializes interface{} as map[string]interface{}
	return index.handleDeserializedData(container)
}

// handleDeserializedData manually handles the Data field deserialization
func (index *flat) handleDeserializedData(container *RQDataContainer) error {
	// Validate version compatibility
	if container.Version > RQDataVersion {
		return errors.Errorf("unsupported RQ data version %d, max supported: %d", container.Version, RQDataVersion)
	}

	// Validate compression type matches current index configuration
	expectedCompressionType := index.getExpectedCompressionType()
	if container.CompressionType != expectedCompressionType {
		return errors.Errorf("compression type mismatch: persisted data is %s but index expects %s",
			container.CompressionType, expectedCompressionType)
	}

	// Handle the Data field based on compression type
	switch container.CompressionType {
	case CompressionTypeRQ1:
		// Deserialize the Data field as RQ1Data
		dataBytes, err := msgpack.Marshal(container.Data)
		if err != nil {
			return errors.Wrap(err, "marshal container data for RQ1")
		}

		var rq1Data RQ1Data
		if err := msgpack.Unmarshal(dataBytes, &rq1Data); err != nil {
			return errors.Wrap(err, "unmarshal RQ1Data")
		}

		index.logger.Warnf("Successfully deserialized RQ1Data: InputDim=%d, OutputDim=%d",
			rq1Data.InputDim, rq1Data.OutputDim)
		return index.restoreRQ1FromMsgpack(&rq1Data)
	case CompressionTypeRQ8:
		// Deserialize the Data field as RQ8Data
		dataBytes, err := msgpack.Marshal(container.Data)
		if err != nil {
			return errors.Wrap(err, "marshal container data for RQ8")
		}

		var rq8Data RQ8Data
		if err := msgpack.Unmarshal(dataBytes, &rq8Data); err != nil {
			return errors.Wrap(err, "unmarshal RQ8Data")
		}

		index.logger.Warnf("Successfully deserialized RQ8Data: InputDim=%d, Bits=%d",
			rq8Data.InputDim, rq8Data.Bits)
		return index.restoreRQ8FromMsgpack(&rq8Data)
	default:
		return errors.Errorf("unsupported compression type: %s", container.CompressionType)
	}
}

// getExpectedCompressionType returns the compression type string for the current index configuration
func (index *flat) getExpectedCompressionType() string {
	switch index.compressionType {
	case CompressionRQ1:
		return CompressionTypeRQ1
	case CompressionRQ8:
		return CompressionTypeRQ8
	default:
		return "" // No RQ compression
	}
}

// restoreRQ1FromMsgpack restores RQ1 quantizer from msgpack data
func (index *flat) restoreRQ1FromMsgpack(rq1Data *RQ1Data) error {
	// Restore the RQ1 quantizer
	rq, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
		int(rq1Data.InputDim),
		int(rq1Data.OutputDim),
		int(rq1Data.Rounds),
		rq1Data.Swaps,
		rq1Data.Signs,
		rq1Data.Rounding,
		index.distancerProvider,
	)
	if err != nil {
		return errors.Wrap(err, "restore binary rotational quantizer from msgpack")
	}

	index.compressed.Store(true)
	index.quantizer = &BinaryRotationalQuantizerWrapper{BinaryRotationalQuantizer: rq}
	return nil
}

// restoreRQ8FromMsgpack restores RQ8 quantizer from msgpack data
func (index *flat) restoreRQ8FromMsgpack(rq8Data *RQ8Data) error {
	// Restore the RQ8 quantizer
	rq, err := compressionhelpers.RestoreRotationalQuantizer(
		int(rq8Data.InputDim),
		int(rq8Data.Bits),
		int(rq8Data.OutputDim),
		int(rq8Data.Rounds),
		rq8Data.Swaps,
		rq8Data.Signs,
		index.distancerProvider,
	)
	if err != nil {
		return errors.Wrap(err, "restore rotational quantizer from msgpack")
	}

	index.compressed.Store(true)
	index.quantizer = &RotationalQuantizerWrapper{RotationalQuantizer: rq}
	return nil
}
