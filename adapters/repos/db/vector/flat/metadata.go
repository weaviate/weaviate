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
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	bolt "go.etcd.io/bbolt"
)

const (
	metadataPrefix       = "meta"
	vectorMetadataBucket = "vector"
)

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

func (index *flat) initMetadata() error {
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
		index.trackDimensionsOnce.Do(func() {
			atomic.StoreInt32(&index.dims, dims)
		})
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
	if (index.compression != CompressionRQ1 && index.compression != CompressionRQ8) || index.quantizer == nil {
		return nil // No RQ data to persist
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

		// Create a simple commit logger to capture RQ data
		logger := &flatCommitLogger{bucket: b}
		index.quantizer.PersistCompression(logger)

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "persist RQ data")
	}

	return nil
}

func (index *flat) restoreRQData() error {
	if index.compression != CompressionRQ1 && index.compression != CompressionRQ8 {
		return nil // No RQ to restore
	}

	err := index.openMetadata()
	if err != nil {
		return err
	}
	defer index.closeMetadata()

	var brqData *compressionhelpers.BRQData
	var rqData *compressionhelpers.RQData
	err = index.metadata.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(vectorMetadataBucket))
		if b == nil {
			return nil // No metadata yet
		}

		// Check if RQ data exists
		data := b.Get([]byte("rq_data"))
		if data == nil {
			return nil // No RQ data to restore
		}

		// Check compression type to determine which data structure to use
		if index.compression == CompressionRQ1 {
			// Handle BRQData for RQ1
			brqData = &compressionhelpers.BRQData{}
			pos := 0

			// Read InputDim
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: InputDim")
			}
			brqData.InputDim = binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Rotation.OutputDim
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: OutputDim")
			}
			outputDim := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Rotation.Rounds
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: Rounds")
			}
			rounds := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Swaps
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: Swaps length")
			}
			swapsLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			brqData.Rotation.Swaps = make([][]compressionhelpers.Swap, swapsLen)
			for i := uint32(0); i < swapsLen; i++ {
				if pos+4 > len(data) {
					return errors.New("invalid RQ data: Swap array length")
				}
				swapLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4

				brqData.Rotation.Swaps[i] = make([]compressionhelpers.Swap, swapLen)
				for j := uint32(0); j < swapLen; j++ {
					if pos+4 > len(data) {
						return errors.New("invalid RQ data: Swap I")
					}
					brqData.Rotation.Swaps[i][j].I = binary.LittleEndian.Uint16(data[pos:])
					pos += 2
					if pos+2 > len(data) {
						return errors.New("invalid RQ data: Swap J")
					}
					brqData.Rotation.Swaps[i][j].J = binary.LittleEndian.Uint16(data[pos:])
					pos += 2
				}
			}

			// Read Signs
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: Signs length")
			}
			signsLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			brqData.Rotation.Signs = make([][]float32, signsLen)
			for i := uint32(0); i < signsLen; i++ {
				if pos+4 > len(data) {
					return errors.New("invalid RQ data: Sign array length")
				}
				signLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4

				brqData.Rotation.Signs[i] = make([]float32, signLen)
				for j := uint32(0); j < signLen; j++ {
					if pos+4 > len(data) {
						return errors.New("invalid RQ data: Sign value")
					}
					brqData.Rotation.Signs[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
					pos += 4
				}
			}

			// Read Rounding
			if pos+4 > len(data) {
				return errors.New("invalid RQ data: Rounding length")
			}
			roundingLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			brqData.Rounding = make([]float32, roundingLen)
			for i := uint32(0); i < roundingLen; i++ {
				if pos+4 > len(data) {
					return errors.New("invalid RQ data: Rounding value")
				}
				brqData.Rounding[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
				pos += 4
			}

			// Set rotation metadata
			brqData.Rotation.OutputDim = outputDim
			brqData.Rotation.Rounds = rounds
		} else if index.compression == CompressionRQ8 {
			// Handle RQData for RQ8
			rqData = &compressionhelpers.RQData{}
			pos := 0

			// Read InputDim
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: InputDim")
			}
			rqData.InputDim = binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Bits
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: Bits")
			}
			rqData.Bits = binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Rotation.OutputDim
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: OutputDim")
			}
			outputDim := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Rotation.Rounds
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: Rounds")
			}
			rounds := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			// Read Swaps
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: Swaps length")
			}
			swapsLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			rqData.Rotation.Swaps = make([][]compressionhelpers.Swap, swapsLen)
			for i := uint32(0); i < swapsLen; i++ {
				if pos+4 > len(data) {
					return errors.New("invalid RQ8 data: Swap array length")
				}
				swapLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4

				rqData.Rotation.Swaps[i] = make([]compressionhelpers.Swap, swapLen)
				for j := uint32(0); j < swapLen; j++ {
					if pos+4 > len(data) {
						return errors.New("invalid RQ8 data: Swap I")
					}
					rqData.Rotation.Swaps[i][j].I = binary.LittleEndian.Uint16(data[pos:])
					pos += 2
					if pos+2 > len(data) {
						return errors.New("invalid RQ8 data: Swap J")
					}
					rqData.Rotation.Swaps[i][j].J = binary.LittleEndian.Uint16(data[pos:])
					pos += 2
				}
			}

			// Read Signs
			if pos+4 > len(data) {
				return errors.New("invalid RQ8 data: Signs length")
			}
			signsLen := binary.LittleEndian.Uint32(data[pos:])
			pos += 4

			rqData.Rotation.Signs = make([][]float32, signsLen)
			for i := uint32(0); i < signsLen; i++ {
				if pos+4 > len(data) {
					return errors.New("invalid RQ8 data: Sign array length")
				}
				signLen := binary.LittleEndian.Uint32(data[pos:])
				pos += 4

				rqData.Rotation.Signs[i] = make([]float32, signLen)
				for j := uint32(0); j < signLen; j++ {
					if pos+4 > len(data) {
						return errors.New("invalid RQ8 data: Sign value")
					}
					rqData.Rotation.Signs[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos:]))
					pos += 4
				}
			}

			// Set rotation metadata
			rqData.Rotation.OutputDim = outputDim
			rqData.Rotation.Rounds = rounds
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "restore RQ data")
	}

	if brqData != nil {
		// Restore the RQ1 quantizer
		rq, err := compressionhelpers.RestoreBinaryRotationalQuantizer(
			int(brqData.InputDim),
			int(brqData.Rotation.OutputDim),
			int(brqData.Rotation.Rounds),
			brqData.Rotation.Swaps,
			brqData.Rotation.Signs,
			brqData.Rounding,
			index.distancerProvider,
		)
		if err != nil {
			return errors.Wrap(err, "restore binary rotational quantizer")
		}
		// Wrap it in the unified interface
		index.quantizer = &BinaryRotationalQuantizerWrapper{BinaryRotationalQuantizer: rq}
	} else if rqData != nil {
		// Restore the RQ8 quantizer
		rq, err := compressionhelpers.RestoreRotationalQuantizer(
			int(rqData.InputDim),
			int(rqData.Bits),
			int(rqData.Rotation.OutputDim),
			int(rqData.Rotation.Rounds),
			rqData.Rotation.Swaps,
			rqData.Rotation.Signs,
			index.distancerProvider,
		)
		if err != nil {
			return errors.Wrap(err, "restore rotational quantizer")
		}
		// Wrap it in the unified interface
		index.quantizer = &RotationalQuantizerWrapper{RotationalQuantizer: rq}
	}

	return nil
}

// flatCommitLogger implements CommitLogger interface for persisting RQ data
type flatCommitLogger struct {
	bucket *bolt.Bucket
}

func (f *flatCommitLogger) AddRQCompression(data compressionhelpers.RQData) error {
	// Serialize RQData to bytes
	var buf []byte

	// Write InputDim
	inputDimBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(inputDimBytes, data.InputDim)
	buf = append(buf, inputDimBytes...)

	// Write Bits
	bitsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bitsBytes, data.Bits)
	buf = append(buf, bitsBytes...)

	// Write Rotation.OutputDim
	outputDimBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(outputDimBytes, data.Rotation.OutputDim)
	buf = append(buf, outputDimBytes...)

	// Write Rotation.Rounds
	roundsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(roundsBytes, data.Rotation.Rounds)
	buf = append(buf, roundsBytes...)

	// Write Swaps
	swapsLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(swapsLenBytes, uint32(len(data.Rotation.Swaps)))
	buf = append(buf, swapsLenBytes...)

	for _, swapArray := range data.Rotation.Swaps {
		swapLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(swapLenBytes, uint32(len(swapArray)))
		buf = append(buf, swapLenBytes...)

		for _, swap := range swapArray {
			iBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(iBytes, swap.I)
			buf = append(buf, iBytes...)

			jBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(jBytes, swap.J)
			buf = append(buf, jBytes...)
		}
	}

	// Write Signs
	signsLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(signsLenBytes, uint32(len(data.Rotation.Signs)))
	buf = append(buf, signsLenBytes...)

	for _, signArray := range data.Rotation.Signs {
		signLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(signLenBytes, uint32(len(signArray)))
		buf = append(buf, signLenBytes...)

		for _, sign := range signArray {
			signBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(signBytes, math.Float32bits(sign))
			buf = append(buf, signBytes...)
		}
	}

	// Store the serialized data
	return f.bucket.Put([]byte("rq_data"), buf)
}

func (f *flatCommitLogger) AddBRQCompression(data compressionhelpers.BRQData) error {
	// Serialize BRQData to bytes
	var buf []byte

	// Write InputDim
	inputDimBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(inputDimBytes, data.InputDim)
	buf = append(buf, inputDimBytes...)

	// Write Rotation.OutputDim
	outputDimBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(outputDimBytes, data.Rotation.OutputDim)
	buf = append(buf, outputDimBytes...)

	// Write Rotation.Rounds
	roundsBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(roundsBytes, uint32(data.Rotation.Rounds))
	buf = append(buf, roundsBytes...)

	// Write Swaps
	swapsLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(swapsLenBytes, uint32(len(data.Rotation.Swaps)))
	buf = append(buf, swapsLenBytes...)

	for _, swapArray := range data.Rotation.Swaps {
		swapLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(swapLenBytes, uint32(len(swapArray)))
		buf = append(buf, swapLenBytes...)

		for _, swap := range swapArray {
			iBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(iBytes, swap.I)
			buf = append(buf, iBytes...)
			jBytes := make([]byte, 2)
			binary.LittleEndian.PutUint16(jBytes, swap.J)
			buf = append(buf, jBytes...)
		}
	}

	// Write Signs
	signsLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(signsLenBytes, uint32(len(data.Rotation.Signs)))
	buf = append(buf, signsLenBytes...)

	for _, signArray := range data.Rotation.Signs {
		signLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(signLenBytes, uint32(len(signArray)))
		buf = append(buf, signLenBytes...)

		for _, sign := range signArray {
			signBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(signBytes, math.Float32bits(sign))
			buf = append(buf, signBytes...)
		}
	}

	// Write Rounding
	roundingLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(roundingLenBytes, uint32(len(data.Rounding)))
	buf = append(buf, roundingLenBytes...)

	for _, rounding := range data.Rounding {
		roundingBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(roundingBytes, math.Float32bits(rounding))
		buf = append(buf, roundingBytes...)
	}

	return f.bucket.Put([]byte("rq_data"), buf)
}

func (f *flatCommitLogger) AddPQCompression(data compressionhelpers.PQData) error {
	// Not used for flat index
	return nil
}

func (f *flatCommitLogger) AddSQCompression(data compressionhelpers.SQData) error {
	// Not used for flat index
	return nil
}
