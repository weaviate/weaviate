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

package compactv2

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/packedconn"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const (
	// snapshotConcurrency is the number of goroutines used for concurrent block reading.
	snapshotConcurrency = 8
)

// ReadSeekReaderAt combines io.Reader, io.Seeker, and io.ReaderAt interfaces.
// This is needed for concurrent block reading using io.SectionReader.
type ReadSeekReaderAt interface {
	io.Reader
	io.Seeker
	io.ReaderAt
}

// SnapshotReader reads V3 format snapshots into DeserializationResult.
// This reader follows the same logic as the hnsw package's snapshot reader
// to ensure format compatibility.
//
// Notes:
//   - Uses concurrent block reading with 8 goroutines for performance
//   - Supports PQ, SQ, RQ, BRQ compression
//   - Supports Muvera encoder
type SnapshotReader struct {
	blockSize int64
	logger    logrus.FieldLogger
}

// NewSnapshotReader creates a new snapshot reader with the default 4MB block size.
func NewSnapshotReader(logger logrus.FieldLogger) *SnapshotReader {
	return &SnapshotReader{
		blockSize: defaultBlockSize,
		logger:    logger,
	}
}

// NewSnapshotReaderWithBlockSize creates a new snapshot reader with a custom block size.
func NewSnapshotReaderWithBlockSize(logger logrus.FieldLogger, blockSize int64) *SnapshotReader {
	return &SnapshotReader{
		blockSize: blockSize,
		logger:    logger,
	}
}

// ReadFromFile reads a snapshot file into a DeserializationResult.
func (r *SnapshotReader) ReadFromFile(filename string) (*DeserializationResult, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "open snapshot file %q", filename)
	}
	defer f.Close()

	return r.Read(f)
}

// Read reads a snapshot from a reader into a DeserializationResult.
// Uses concurrent block reading with multiple goroutines for optimal performance.
// The reader must implement io.Reader, io.Seeker, and io.ReaderAt for concurrent access.
func (r *SnapshotReader) Read(reader ReadSeekReaderAt) (*DeserializationResult, error) {
	res := &DeserializationResult{
		NodesDeleted:      make(map[uint64]struct{}),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: make(map[uint64]struct{}),
		LinksReplaced:     make(map[uint64]map[uint16]struct{}),
	}

	// Read and verify metadata
	if err := r.readMetadata(reader, res); err != nil {
		return nil, errors.Wrap(err, "read metadata")
	}

	// Read body with concurrent block reading
	if err := r.readBodyConcurrent(reader, res); err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	return res, nil
}

// readMetadata reads the snapshot metadata header.
func (r *SnapshotReader) readMetadata(reader io.Reader, res *DeserializationResult) error {
	// Read version
	var version uint8
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return errors.Wrap(err, "read version")
	}
	if version != snapshotVersionV3 {
		return fmt.Errorf("unsupported snapshot version %d, expected %d", version, snapshotVersionV3)
	}

	// Read checksum
	var checksum uint32
	if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
		return errors.Wrap(err, "read checksum")
	}

	// Read metadata size
	var metadataSize uint32
	if err := binary.Read(reader, binary.LittleEndian, &metadataSize); err != nil {
		return errors.Wrap(err, "read metadata size")
	}

	// Read metadata
	metadata := make([]byte, metadataSize)
	if _, err := io.ReadFull(reader, metadata); err != nil {
		return errors.Wrap(err, "read metadata")
	}

	// Verify checksum
	hasher := crc32.NewIEEE()
	var versionBuf [1]byte
	versionBuf[0] = version
	_, _ = hasher.Write(versionBuf[:])
	var metaSizeBuf [4]byte
	binary.LittleEndian.PutUint32(metaSizeBuf[:], metadataSize)
	_, _ = hasher.Write(metaSizeBuf[:])
	_, _ = hasher.Write(metadata)
	if hasher.Sum32() != checksum {
		return fmt.Errorf("metadata checksum mismatch")
	}

	// Parse metadata
	metaReader := bytes.NewReader(metadata)

	// Entrypoint
	if err := binary.Read(metaReader, binary.LittleEndian, &res.Entrypoint); err != nil {
		return errors.Wrap(err, "read entrypoint")
	}

	// Level
	if err := binary.Read(metaReader, binary.LittleEndian, &res.Level); err != nil {
		return errors.Wrap(err, "read level")
	}

	// isCompressed
	var isCompressed uint8
	if err := binary.Read(metaReader, binary.LittleEndian, &isCompressed); err != nil {
		return errors.Wrap(err, "read isCompressed")
	}
	res.Compressed = isCompressed != 0

	if res.Compressed {
		if err := r.readCompressionData(metaReader, res); err != nil {
			return errors.Wrap(err, "read compression data")
		}
	}

	// isEncoded (Muvera)
	var isEncoded uint8
	if err := binary.Read(metaReader, binary.LittleEndian, &isEncoded); err != nil {
		return errors.Wrap(err, "read isEncoded")
	}
	res.MuveraEnabled = isEncoded != 0

	if res.MuveraEnabled {
		if err := r.readMuveraData(metaReader, res); err != nil {
			return errors.Wrap(err, "read muvera data")
		}
	}

	// Node count
	var nodeCount uint32
	if err := binary.Read(metaReader, binary.LittleEndian, &nodeCount); err != nil {
		return errors.Wrap(err, "read node count")
	}

	// Pre-allocate nodes slice
	res.Nodes = make([]*Vertex, nodeCount)

	res.EntrypointChanged = true

	return nil
}

// readCompressionData reads compression data from the metadata.
func (r *SnapshotReader) readCompressionData(reader io.Reader, res *DeserializationResult) error {
	// Read compression type
	var compressionType uint8
	if err := binary.Read(reader, binary.LittleEndian, &compressionType); err != nil {
		return errors.Wrap(err, "read compression type")
	}

	switch compressionType {
	case SnapshotCompressionTypePQ:
		return r.readPQData(reader, res)
	case SnapshotCompressionTypeSQ:
		return r.readSQData(reader, res)
	case SnapshotCompressionTypeRQ:
		return r.readRQData(reader, res)
	case SnapshotCompressionTypeBRQ:
		return r.readBRQData(reader, res)
	default:
		return fmt.Errorf("unsupported compression type %d", compressionType)
	}
}

// readPQData reads Product Quantization data from the reader.
func (r *SnapshotReader) readPQData(reader io.Reader, res *DeserializationResult) error {
	var dims, ks, m uint16
	var encoderType, dist uint8
	var useBitsEncoding uint8

	if err := binary.Read(reader, binary.LittleEndian, &dims); err != nil {
		return errors.Wrap(err, "read PQ dimensions")
	}
	if err := binary.Read(reader, binary.LittleEndian, &ks); err != nil {
		return errors.Wrap(err, "read PQ Ks")
	}
	if err := binary.Read(reader, binary.LittleEndian, &m); err != nil {
		return errors.Wrap(err, "read PQ M")
	}
	if err := binary.Read(reader, binary.LittleEndian, &encoderType); err != nil {
		return errors.Wrap(err, "read PQ encoder type")
	}
	if err := binary.Read(reader, binary.LittleEndian, &dist); err != nil {
		return errors.Wrap(err, "read PQ distribution")
	}
	if err := binary.Read(reader, binary.LittleEndian, &useBitsEncoding); err != nil {
		return errors.Wrap(err, "read PQ useBitsEncoding")
	}

	pqData := &compressionhelpers.PQData{
		Dimensions:          dims,
		EncoderType:         compressionhelpers.Encoder(encoderType),
		Ks:                  ks,
		M:                   m,
		EncoderDistribution: dist,
		UseBitsEncoding:     useBitsEncoding != 0,
	}

	// Read encoders
	var encoderReader func(io.Reader, *compressionhelpers.PQData, uint16) (compressionhelpers.PQEncoder, error)
	switch pqData.EncoderType {
	case compressionhelpers.UseTileEncoder:
		encoderReader = readSnapshotTileEncoder
	case compressionhelpers.UseKMeansEncoder:
		encoderReader = readSnapshotKMeansEncoder
	default:
		return fmt.Errorf("unsupported PQ encoder type %d", encoderType)
	}

	for i := uint16(0); i < m; i++ {
		encoder, err := encoderReader(reader, pqData, i)
		if err != nil {
			return errors.Wrapf(err, "read PQ encoder %d", i)
		}
		pqData.Encoders = append(pqData.Encoders, encoder)
	}

	res.CompressionPQData = pqData
	return nil
}

// readSnapshotTileEncoder reads a tile encoder from the snapshot.
func readSnapshotTileEncoder(reader io.Reader, data *compressionhelpers.PQData, i uint16) (compressionhelpers.PQEncoder, error) {
	bins, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}
	mean, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}
	stdDev, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}
	size, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}
	s1, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}
	s2, err := readSnapshotFloat64(reader)
	if err != nil {
		return nil, err
	}

	var segment uint16
	if err := binary.Read(reader, binary.LittleEndian, &segment); err != nil {
		return nil, errors.Wrap(err, "read segment")
	}

	var encDistribution uint8
	if err := binary.Read(reader, binary.LittleEndian, &encDistribution); err != nil {
		return nil, errors.Wrap(err, "read encDistribution")
	}

	return compressionhelpers.RestoreTileEncoder(bins, mean, stdDev, size, s1, s2, segment, encDistribution), nil
}

// readSnapshotKMeansEncoder reads a KMeans encoder from the snapshot.
func readSnapshotKMeansEncoder(reader io.Reader, data *compressionhelpers.PQData, i uint16) (compressionhelpers.PQEncoder, error) {
	ds := int(data.Dimensions / data.M)
	centers := make([][]float32, 0, data.Ks)

	for k := uint16(0); k < data.Ks; k++ {
		center := make([]float32, 0, ds)
		for j := 0; j < ds; j++ {
			var bits uint32
			if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
				return nil, errors.Wrap(err, "read center value")
			}
			center = append(center, math.Float32frombits(bits))
		}
		centers = append(centers, center)
	}

	return compressionhelpers.NewKMeansEncoderWithCenters(int(data.Ks), ds, int(i), centers), nil
}

// readSnapshotFloat64 reads a float64 from the reader.
func readSnapshotFloat64(reader io.Reader) (float64, error) {
	var bits uint64
	if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
		return 0, errors.Wrap(err, "read float64")
	}
	return math.Float64frombits(bits), nil
}

// readSQData reads Scalar Quantization data from the reader.
func (r *SnapshotReader) readSQData(reader io.Reader, res *DeserializationResult) error {
	var dims uint16
	var aBits, bBits uint32

	if err := binary.Read(reader, binary.LittleEndian, &dims); err != nil {
		return errors.Wrap(err, "read SQ dimensions")
	}
	if err := binary.Read(reader, binary.LittleEndian, &aBits); err != nil {
		return errors.Wrap(err, "read SQ A")
	}
	if err := binary.Read(reader, binary.LittleEndian, &bBits); err != nil {
		return errors.Wrap(err, "read SQ B")
	}

	res.CompressionSQData = &compressionhelpers.SQData{
		Dimensions: dims,
		A:          math.Float32frombits(aBits),
		B:          math.Float32frombits(bBits),
	}
	return nil
}

// readRQData reads Rotational Quantization data from the reader.
func (r *SnapshotReader) readRQData(reader io.Reader, res *DeserializationResult) error {
	var inputDim, bits, outputDim, rounds uint32

	if err := binary.Read(reader, binary.LittleEndian, &inputDim); err != nil {
		return errors.Wrap(err, "read RQ inputDim")
	}
	if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
		return errors.Wrap(err, "read RQ bits")
	}
	if err := binary.Read(reader, binary.LittleEndian, &outputDim); err != nil {
		return errors.Wrap(err, "read RQ outputDim")
	}
	if err := binary.Read(reader, binary.LittleEndian, &rounds); err != nil {
		return errors.Wrap(err, "read RQ rounds")
	}

	// Read swaps
	swaps := make([][]compressionhelpers.Swap, rounds)
	for i := uint32(0); i < rounds; i++ {
		swaps[i] = make([]compressionhelpers.Swap, outputDim/2)
		for j := uint32(0); j < outputDim/2; j++ {
			if err := binary.Read(reader, binary.LittleEndian, &swaps[i][j].I); err != nil {
				return errors.Wrap(err, "read RQ swap I")
			}
			if err := binary.Read(reader, binary.LittleEndian, &swaps[i][j].J); err != nil {
				return errors.Wrap(err, "read RQ swap J")
			}
		}
	}

	// Read signs
	signs := make([][]float32, rounds)
	for i := uint32(0); i < rounds; i++ {
		signs[i] = make([]float32, outputDim)
		for j := uint32(0); j < outputDim; j++ {
			var bits uint32
			if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
				return errors.Wrap(err, "read RQ sign")
			}
			signs[i][j] = math.Float32frombits(bits)
		}
	}

	res.CompressionRQData = &compressionhelpers.RQData{
		InputDim: inputDim,
		Bits:     bits,
		Rotation: compressionhelpers.FastRotation{
			OutputDim: outputDim,
			Rounds:    rounds,
			Swaps:     swaps,
			Signs:     signs,
		},
	}
	return nil
}

// readBRQData reads Binary Rotational Quantization data from the reader.
func (r *SnapshotReader) readBRQData(reader io.Reader, res *DeserializationResult) error {
	var inputDim, outputDim, rounds uint32

	if err := binary.Read(reader, binary.LittleEndian, &inputDim); err != nil {
		return errors.Wrap(err, "read BRQ inputDim")
	}
	if err := binary.Read(reader, binary.LittleEndian, &outputDim); err != nil {
		return errors.Wrap(err, "read BRQ outputDim")
	}
	if err := binary.Read(reader, binary.LittleEndian, &rounds); err != nil {
		return errors.Wrap(err, "read BRQ rounds")
	}

	// Read swaps
	swaps := make([][]compressionhelpers.Swap, rounds)
	for i := uint32(0); i < rounds; i++ {
		swaps[i] = make([]compressionhelpers.Swap, outputDim/2)
		for j := uint32(0); j < outputDim/2; j++ {
			if err := binary.Read(reader, binary.LittleEndian, &swaps[i][j].I); err != nil {
				return errors.Wrap(err, "read BRQ swap I")
			}
			if err := binary.Read(reader, binary.LittleEndian, &swaps[i][j].J); err != nil {
				return errors.Wrap(err, "read BRQ swap J")
			}
		}
	}

	// Read signs
	signs := make([][]float32, rounds)
	for i := uint32(0); i < rounds; i++ {
		signs[i] = make([]float32, outputDim)
		for j := uint32(0); j < outputDim; j++ {
			var bits uint32
			if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
				return errors.Wrap(err, "read BRQ sign")
			}
			signs[i][j] = math.Float32frombits(bits)
		}
	}

	// Read rounding
	rounding := make([]float32, outputDim)
	for i := uint32(0); i < outputDim; i++ {
		var bits uint32
		if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
			return errors.Wrap(err, "read BRQ rounding")
		}
		rounding[i] = math.Float32frombits(bits)
	}

	res.CompressionBRQData = &compressionhelpers.BRQData{
		InputDim: inputDim,
		Rotation: compressionhelpers.FastRotation{
			OutputDim: outputDim,
			Rounds:    rounds,
			Swaps:     swaps,
			Signs:     signs,
		},
		Rounding: rounding,
	}
	return nil
}

// readMuveraData reads Muvera encoder data from the reader.
func (r *SnapshotReader) readMuveraData(reader io.Reader, res *DeserializationResult) error {
	// Read encoder type
	var encoderType uint8
	if err := binary.Read(reader, binary.LittleEndian, &encoderType); err != nil {
		return errors.Wrap(err, "read encoder type")
	}

	if encoderType != SnapshotEncoderTypeMuvera {
		return fmt.Errorf("unsupported encoder type %d", encoderType)
	}

	var dims, kSim, numClusters, dProjections, repetitions uint32

	if err := binary.Read(reader, binary.LittleEndian, &dims); err != nil {
		return errors.Wrap(err, "read Muvera dimensions")
	}
	if err := binary.Read(reader, binary.LittleEndian, &kSim); err != nil {
		return errors.Wrap(err, "read Muvera kSim")
	}
	if err := binary.Read(reader, binary.LittleEndian, &numClusters); err != nil {
		return errors.Wrap(err, "read Muvera numClusters")
	}
	if err := binary.Read(reader, binary.LittleEndian, &dProjections); err != nil {
		return errors.Wrap(err, "read Muvera dProjections")
	}
	if err := binary.Read(reader, binary.LittleEndian, &repetitions); err != nil {
		return errors.Wrap(err, "read Muvera repetitions")
	}

	// Read gaussians
	gaussians := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		gaussians[i] = make([][]float32, kSim)
		for j := uint32(0); j < kSim; j++ {
			gaussians[i][j] = make([]float32, dims)
			for k := uint32(0); k < dims; k++ {
				var bits uint32
				if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
					return errors.Wrap(err, "read Muvera gaussian")
				}
				gaussians[i][j][k] = math.Float32frombits(bits)
			}
		}
	}

	// Read S matrices
	s := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		s[i] = make([][]float32, dProjections)
		for j := uint32(0); j < dProjections; j++ {
			s[i][j] = make([]float32, dims)
			for k := uint32(0); k < dims; k++ {
				var bits uint32
				if err := binary.Read(reader, binary.LittleEndian, &bits); err != nil {
					return errors.Wrap(err, "read Muvera S")
				}
				s[i][j][k] = math.Float32frombits(bits)
			}
		}
	}

	res.EncoderMuvera = &multivector.MuveraData{
		Dimensions:   dims,
		NumClusters:  numClusters,
		KSim:         kSim,
		DProjections: dProjections,
		Repetitions:  repetitions,
		Gaussians:    gaussians,
		S:            s,
	}
	return nil
}

// readBodyConcurrent reads the snapshot body blocks concurrently using multiple goroutines.
// This significantly improves startup performance for large indexes.
func (r *SnapshotReader) readBodyConcurrent(reader ReadSeekReaderAt, res *DeserializationResult) error {
	if len(res.Nodes) == 0 {
		return nil
	}

	// Get current position and file size to determine body size
	currentPos, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return errors.Wrap(err, "get current position")
	}

	endPos, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "seek to end")
	}

	bodySize := int(endPos - currentPos)
	if bodySize == 0 {
		return nil
	}

	// Seek back to where we were (body start)
	if _, err := reader.Seek(currentPos, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek back")
	}

	// Setup concurrent block reading
	var mu sync.Mutex
	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(r.logger, context.Background())
	eg.SetLimit(snapshotConcurrency)

	// Channel for distributing block offsets to workers
	ch := make(chan int, snapshotConcurrency)

	// Start worker goroutines
	for i := 0; i < snapshotConcurrency; i++ {
		eg.Go(func() error {
			buf := make([]byte, r.blockSize)

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case offset, ok := <-ch:
					if !ok {
						return nil // channel closed, done
					}

					// Read block using SectionReader for concurrent access
					sr := io.NewSectionReader(reader, currentPos+int64(offset), r.blockSize)
					n, err := io.ReadFull(sr, buf)
					if err != nil {
						if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
							return errors.Wrapf(err, "read block at offset %d", offset)
						}
						if n == 0 {
							return nil
						}
					}

					// Parse block and update result with mutex protection
					if err := r.readBlockConcurrent(buf[:n], res, &mu); err != nil {
						return errors.Wrapf(err, "parse block at offset %d", offset)
					}
				}
			}
		})
	}

	// Send block offsets to workers
LOOP:
	for i := 0; i < bodySize; i += int(r.blockSize) {
		select {
		case <-ctx.Done():
			break LOOP
		case ch <- i:
		}
	}
	close(ch)

	// Wait for all workers to complete
	return eg.Wait()
}

// readBlockConcurrent parses a single block and populates nodes in the result.
// Uses mutex protection for concurrent access to the result.
func (r *SnapshotReader) readBlockConcurrent(buf []byte, res *DeserializationResult, mu *sync.Mutex) error {
	if len(buf) < 8 {
		return fmt.Errorf("block too small: %d bytes", len(buf))
	}

	// Verify checksum
	blockChecksum := binary.LittleEndian.Uint32(buf[:4])
	hasher := crc32.NewIEEE()
	_, _ = hasher.Write(buf[4:])
	if hasher.Sum32() != blockChecksum {
		return fmt.Errorf("block checksum mismatch")
	}

	// Read block length from end
	blockLen := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	block := buf[4 : 4+blockLen]

	// Use byteops.ReadWriter instead of bytes.Reader + binary.Read
	rw := byteops.NewReadWriter(block)

	// Read start node ID
	startNodeID := rw.ReadUint64()
	currNodeID := startNodeID

	// Read nodes
	for rw.Position < uint64(len(block)) {
		existence := rw.ReadUint8()

		if existence == 0 {
			// nil node
			currNodeID++
			continue
		}

		// Read level
		level := rw.ReadUint32()

		// Read connections size
		connSize := rw.ReadUint32()

		// Read connections data (use subslice to avoid copying)
		connData := rw.ReadBytesFromBuffer(uint64(connSize))

		// Create node
		if currNodeID < uint64(len(res.Nodes)) {
			// Copy connData since ReadBytesFromBuffer returns a slice of the underlying buffer
			connDataCopy := make([]byte, len(connData))
			copy(connDataCopy, connData)

			node := &Vertex{
				ID:          currNodeID,
				Level:       int(level),
				Connections: packedconn.NewWithData(connDataCopy),
			}

			// Update result with mutex protection
			mu.Lock()
			res.Nodes[currNodeID] = node
			if existence == 1 {
				res.Tombstones[currNodeID] = struct{}{}
			}
			mu.Unlock()
		}

		currNodeID++
	}

	return nil
}
