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
	"bufio"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// Use the same logical limit as the stateful deserializer, but local to this file.
const maxConnectionsPerNodeReader = 4096

// ---------------------------------------------------------------------------
// Commit interface + concrete commit types
// ---------------------------------------------------------------------------

type Commit interface {
	Type() HnswCommitType
}

// Node / entrypoint / links

type AddNodeCommit struct {
	ID    uint64
	Level uint16
}

func (c *AddNodeCommit) Type() HnswCommitType { return AddNode }

type SetEntryPointMaxLevelCommit struct {
	Entrypoint uint64
	Level      uint16
}

func (c *SetEntryPointMaxLevelCommit) Type() HnswCommitType { return SetEntryPointMaxLevel }

type AddLinkAtLevelCommit struct {
	Source uint64
	Level  uint16
	Target uint64
}

func (c *AddLinkAtLevelCommit) Type() HnswCommitType { return AddLinkAtLevel }

type AddLinksAtLevelCommit struct {
	Source  uint64
	Level   uint16
	Targets []uint64
}

func (c *AddLinksAtLevelCommit) Type() HnswCommitType { return AddLinksAtLevel }

type ReplaceLinksAtLevelCommit struct {
	Source  uint64
	Level   uint16
	Targets []uint64
}

func (c *ReplaceLinksAtLevelCommit) Type() HnswCommitType { return ReplaceLinksAtLevel }

// Tombstones / deletes

type AddTombstoneCommit struct {
	ID uint64
}

func (c *AddTombstoneCommit) Type() HnswCommitType { return AddTombstone }

type RemoveTombstoneCommit struct {
	ID uint64
}

func (c *RemoveTombstoneCommit) Type() HnswCommitType { return RemoveTombstone }

type ClearLinksCommit struct {
	ID uint64
}

func (c *ClearLinksCommit) Type() HnswCommitType { return ClearLinks }

type ClearLinksAtLevelCommit struct {
	ID    uint64
	Level uint16
}

func (c *ClearLinksAtLevelCommit) Type() HnswCommitType { return ClearLinksAtLevel }

type DeleteNodeCommit struct {
	ID uint64
}

func (c *DeleteNodeCommit) Type() HnswCommitType { return DeleteNode }

type ResetIndexCommit struct{}

func (c *ResetIndexCommit) Type() HnswCommitType { return ResetIndex }

// Compression-related commits

type AddPQCommit struct {
	Data *compression.PQData
}

func (c *AddPQCommit) Type() HnswCommitType { return AddPQ }

type AddSQCommit struct {
	Data *compression.SQData
}

func (c *AddSQCommit) Type() HnswCommitType { return AddSQ }

type AddRQCommit struct {
	Data *compression.RQData
}

func (c *AddRQCommit) Type() HnswCommitType { return AddRQ }

type AddBRQCommit struct {
	Data *compression.BRQData
}

func (c *AddBRQCommit) Type() HnswCommitType { return AddBRQ }

type AddMuveraCommit struct {
	Data *multivector.MuveraData
}

func (c *AddMuveraCommit) Type() HnswCommitType { return AddMuvera }

// ---------------------------------------------------------------------------
// WALCommitReader
// ---------------------------------------------------------------------------

// WALCommitReader streams fully decoded commits from a WAL.
// It does NOT apply them to any in-memory HNSW state.
type WALCommitReader struct {
	r      *bufio.Reader
	logger logrus.FieldLogger

	reusableBuf     []byte
	reusableUint64s []uint64
}

// NewWALCommitReader wraps an io.Reader. Caller controls flow by repeatedly
// calling ReadNextCommit().
func NewWALCommitReader(r io.Reader, logger logrus.FieldLogger) *WALCommitReader {
	if br, ok := r.(*bufio.Reader); ok {
		return &WALCommitReader{
			r:      br,
			logger: logger,
		}
	}
	return &WALCommitReader{
		r:      bufio.NewReader(r),
		logger: logger,
	}
}

// ReadNextCommit returns the next commit in the WAL.
//   - (Commit, nil) on success
//   - (nil, io.EOF) when no more commits are available
//   - (nil, err) on other errors
func (w *WALCommitReader) ReadNextCommit() (Commit, error) {
	ct, err := readCommitType(w.r)
	if err != nil {
		return nil, err
	}

	switch ct {
	case AddNode:
		return w.readAddNode()
	case SetEntryPointMaxLevel:
		return w.readSetEntryPointMaxLevel()
	case AddLinkAtLevel:
		return w.readAddLinkAtLevel()
	case AddLinksAtLevel:
		return w.readAddLinksAtLevel()
	case ReplaceLinksAtLevel:
		return w.readReplaceLinksAtLevel()
	case AddTombstone:
		return w.readAddTombstone()
	case RemoveTombstone:
		return w.readRemoveTombstone()
	case ClearLinks:
		return w.readClearLinks()
	case ClearLinksAtLevel:
		return w.readClearLinksAtLevel()
	case DeleteNode:
		return w.readDeleteNode()
	case ResetIndex:
		return &ResetIndexCommit{}, nil
	case AddPQ:
		return w.readAddPQ()
	case AddSQ:
		return w.readAddSQ()
	case AddMuvera:
		return w.readAddMuvera()
	case AddRQ:
		return w.readAddRQ()
	case AddBRQ:
		return w.readAddBRQ()
	default:
		return nil, errors.Errorf("unrecognized commit type %d", ct)
	}
}

// ---------------------------------------------------------------------------
// internal buffer helpers
// ---------------------------------------------------------------------------

func (w *WALCommitReader) resetBuf(size int) {
	if size <= cap(w.reusableBuf) {
		w.reusableBuf = w.reusableBuf[:size]
	} else {
		w.reusableBuf = make([]byte, size, size*2)
	}
}

func (w *WALCommitReader) resetUint64Slice(size int) {
	if size <= cap(w.reusableUint64s) {
		w.reusableUint64s = w.reusableUint64s[:size]
	} else {
		w.reusableUint64s = make([]uint64, size, size*2)
	}
}

// readUint64 uses the reader's reusable buffer.
func (w *WALCommitReader) readUint64(r io.Reader) (uint64, error) {
	w.resetBuf(8)
	if _, err := io.ReadFull(r, w.reusableBuf); err != nil {
		return 0, errors.Wrap(err, "failed to read uint64")
	}
	return binary.LittleEndian.Uint64(w.reusableBuf), nil
}

func (w *WALCommitReader) readUint64Slice(r io.Reader, length int) ([]uint64, error) {
	w.resetBuf(length * 8)
	w.resetUint64Slice(length)

	if _, err := io.ReadFull(r, w.reusableBuf); err != nil {
		return nil, errors.Wrap(err, "failed to read uint64 slice")
	}

	for i := range w.reusableUint64s {
		w.reusableUint64s[i] = binary.LittleEndian.Uint64(w.reusableBuf[i*8 : (i+1)*8])
	}

	return w.reusableUint64s, nil
}

// ---------------------------------------------------------------------------
// basic primitive readers (copied from deserializer file)
// ---------------------------------------------------------------------------

func readFloat64(r io.Reader) (float64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read float64")
	}
	bits := binary.LittleEndian.Uint64(b[:])
	return math.Float64frombits(bits), nil
}

func readFloat32(r io.Reader) (float32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read float32")
	}
	bits := binary.LittleEndian.Uint32(b[:])
	return math.Float32frombits(bits), nil
}

func readUint16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read uint16")
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func readUint32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read uint32")
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, errors.Wrap(err, "failed to read byte")
	}
	return b[0], nil
}

func readCommitType(r io.Reader) (HnswCommitType, error) {
	b, err := readByte(r)
	if err != nil {
		return 0, errors.Wrap(err, "failed to read commit type")
	}
	return HnswCommitType(b), nil
}

// ---------------------------------------------------------------------------
// per-commit decoding (pure, no state)
// ---------------------------------------------------------------------------

func (w *WALCommitReader) readAddNode() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	level, err := readUint16(w.r)
	if err != nil {
		return nil, err
	}
	return &AddNodeCommit{ID: id, Level: level}, nil
}

func (w *WALCommitReader) readSetEntryPointMaxLevel() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	level, err := readUint16(w.r)
	if err != nil {
		return nil, err
	}
	return &SetEntryPointMaxLevelCommit{
		Entrypoint: id,
		Level:      level,
	}, nil
}

func (w *WALCommitReader) readAddLinkAtLevel() (Commit, error) {
	source, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	level, err := readUint16(w.r)
	if err != nil {
		return nil, err
	}
	target, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	return &AddLinkAtLevelCommit{
		Source: source,
		Level:  level,
		Target: target,
	}, nil
}

// shared between AddLinksAtLevel and ReplaceLinksAtLevel
func (w *WALCommitReader) readLinksHeaderAndTargets() (source uint64, level uint16, targets []uint64, err error) {
	// header: 8 bytes source, 2 bytes level, 2 bytes length
	w.resetBuf(12)
	if _, err = io.ReadFull(w.r, w.reusableBuf); err != nil {
		return 0, 0, nil, err
	}

	source = binary.LittleEndian.Uint64(w.reusableBuf[0:8])
	level = binary.LittleEndian.Uint16(w.reusableBuf[8:10])
	length := binary.LittleEndian.Uint16(w.reusableBuf[10:12])

	rawTargets, err := w.readUint64Slice(w.r, int(length))
	if err != nil {
		return 0, 0, nil, err
	}

	if len(rawTargets) >= maxConnectionsPerNodeReader {
		w.logger.Warnf("read links with %v (>= %d) connections for node %d at level %d, truncating to %d",
			len(rawTargets), maxConnectionsPerNodeReader, source, level, maxConnectionsPerNodeReader)
		rawTargets = rawTargets[:maxConnectionsPerNodeReader]
	}

	// copy into commit-owned slice so it remains valid after next call
	targets = make([]uint64, len(rawTargets))
	copy(targets, rawTargets)

	return source, level, targets, nil
}

func (w *WALCommitReader) readAddLinksAtLevel() (Commit, error) {
	source, level, targets, err := w.readLinksHeaderAndTargets()
	if err != nil {
		return nil, err
	}
	return &AddLinksAtLevelCommit{
		Source:  source,
		Level:   level,
		Targets: targets,
	}, nil
}

func (w *WALCommitReader) readReplaceLinksAtLevel() (Commit, error) {
	source, level, targets, err := w.readLinksHeaderAndTargets()
	if err != nil {
		return nil, err
	}
	return &ReplaceLinksAtLevelCommit{
		Source:  source,
		Level:   level,
		Targets: targets,
	}, nil
}

func (w *WALCommitReader) readAddTombstone() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	return &AddTombstoneCommit{ID: id}, nil
}

func (w *WALCommitReader) readRemoveTombstone() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	return &RemoveTombstoneCommit{ID: id}, nil
}

func (w *WALCommitReader) readClearLinks() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	return &ClearLinksCommit{ID: id}, nil
}

func (w *WALCommitReader) readClearLinksAtLevel() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	level, err := readUint16(w.r)
	if err != nil {
		return nil, err
	}
	return &ClearLinksAtLevelCommit{
		ID:    id,
		Level: level,
	}, nil
}

func (w *WALCommitReader) readDeleteNode() (Commit, error) {
	id, err := w.readUint64(w.r)
	if err != nil {
		return nil, err
	}
	return &DeleteNodeCommit{ID: id}, nil
}

// ---------------------------------------------------------------------------
// Compression readers (copied, but return *Data instead of mutating state)
// ---------------------------------------------------------------------------

func readTileEncoder(r io.Reader, data *compression.PQData, i uint16) (compression.PQSegmentEncoder, error) {
	bins, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	mean, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	stdDev, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	size, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	s1, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	s2, err := readFloat64(r)
	if err != nil {
		return nil, err
	}
	segment, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	encDistribution, err := readByte(r)
	if err != nil {
		return nil, err
	}
	return compressionhelpers.RestoreTileEncoder(bins, mean, stdDev, size, s1, s2, segment, encDistribution), nil
}

func readKMeansEncoder(r io.Reader, data *compression.PQData, i uint16) (compression.PQSegmentEncoder, error) {
	ds := int(data.Dimensions / data.M)
	centers := make([][]float32, 0, data.Ks)
	for k := uint16(0); k < data.Ks; k++ {
		center := make([]float32, 0, ds)
		for i := 0; i < ds; i++ {
			c, err := readFloat32(r)
			if err != nil {
				return nil, err
			}
			center = append(center, c)
		}
		centers = append(centers, center)
	}
	kms := compressionhelpers.NewKMeansEncoderWithCenters(
		int(data.Ks),
		ds,
		int(i),
		centers,
	)
	return kms, nil
}

// PQ

func readPQData(r io.Reader) (*compression.PQData, error) {
	dims, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	encByte, err := readByte(r)
	if err != nil {
		return nil, err
	}
	ks, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	m, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	dist, err := readByte(r)
	if err != nil {
		return nil, err
	}
	useBitsEncoding, err := readByte(r)
	if err != nil {
		return nil, err
	}

	encoder := compression.Encoder(encByte)
	pqData := compression.PQData{
		Dimensions:          dims,
		EncoderType:         encoder,
		Ks:                  ks,
		M:                   m,
		EncoderDistribution: byte(dist),
		UseBitsEncoding:     useBitsEncoding != 0,
	}

	var encoderReader func(io.Reader, *compression.PQData, uint16) (compression.PQSegmentEncoder, error)

	switch encoder {
	case compression.UseTileEncoder:
		encoderReader = readTileEncoder
	case compression.UseKMeansEncoder:
		encoderReader = readKMeansEncoder
	default:
		return nil, errors.New("unsupported encoder type")
	}

	for i := uint16(0); i < m; i++ {
		enc, err := encoderReader(r, &pqData, i)
		if err != nil {
			return nil, err
		}
		pqData.Encoders = append(pqData.Encoders, enc)
	}

	return &pqData, nil
}

// SQ

func readSQData(r io.Reader) (*compression.SQData, error) {
	a, err := readFloat32(r)
	if err != nil {
		return nil, err
	}
	b, err := readFloat32(r)
	if err != nil {
		return nil, err
	}
	dims, err := readUint16(r)
	if err != nil {
		return nil, err
	}
	return &compression.SQData{
		A:          a,
		B:          b,
		Dimensions: dims,
	}, nil
}

// RQ

func readRQData(r io.Reader) (*compression.RQData, error) {
	inputDim, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	bits, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	outputDim, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	rounds, err := readUint32(r)
	if err != nil {
		return nil, err
	}

	swaps := make([][]compression.Swap, rounds)
	for i := uint32(0); i < rounds; i++ {
		swaps[i] = make([]compression.Swap, outputDim/2)
		for j := uint32(0); j < outputDim/2; j++ {
			swaps[i][j].I, err = readUint16(r)
			if err != nil {
				return nil, err
			}
			swaps[i][j].J, err = readUint16(r)
			if err != nil {
				return nil, err
			}
		}
	}

	signs := make([][]float32, rounds)
	for i := uint32(0); i < rounds; i++ {
		signs[i] = make([]float32, outputDim)
		for j := uint32(0); j < outputDim; j++ {
			sign, err := readFloat32(r)
			if err != nil {
				return nil, err
			}
			signs[i][j] = sign
		}
	}

	return &compression.RQData{
		InputDim: inputDim,
		Bits:     bits,
		Rotation: compression.FastRotation{
			OutputDim: outputDim,
			Rounds:    rounds,
			Swaps:     swaps,
			Signs:     signs,
		},
	}, nil
}

// BRQ

func readBRQData(r io.Reader) (*compression.BRQData, error) {
	inputDim, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	outputDim, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	rounds, err := readUint32(r)
	if err != nil {
		return nil, err
	}

	swaps := make([][]compression.Swap, rounds)
	for i := uint32(0); i < rounds; i++ {
		swaps[i] = make([]compression.Swap, outputDim/2)
		for j := uint32(0); j < outputDim/2; j++ {
			swaps[i][j].I, err = readUint16(r)
			if err != nil {
				return nil, err
			}
			swaps[i][j].J, err = readUint16(r)
			if err != nil {
				return nil, err
			}
		}
	}

	signs := make([][]float32, rounds)
	for i := uint32(0); i < rounds; i++ {
		signs[i] = make([]float32, outputDim)
		for j := uint32(0); j < outputDim; j++ {
			sign, err := readFloat32(r)
			if err != nil {
				return nil, err
			}
			signs[i][j] = sign
		}
	}

	rounding := make([]float32, outputDim)
	for i := uint32(0); i < outputDim; i++ {
		rounding[i], err = readFloat32(r)
		if err != nil {
			return nil, err
		}
	}

	return &compression.BRQData{
		InputDim: inputDim,
		Rotation: compression.FastRotation{
			OutputDim: outputDim,
			Rounds:    rounds,
			Swaps:     swaps,
			Signs:     signs,
		},
		Rounding: rounding,
	}, nil
}

// Muvera

func readMuveraData(r io.Reader) (*multivector.MuveraData, error) {
	kSim, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	numClusters, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	dimensions, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	dProjections, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	repetitions, err := readUint32(r)
	if err != nil {
		return nil, err
	}

	gaussians := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		gaussians[i] = make([][]float32, kSim)
		for j := uint32(0); j < kSim; j++ {
			gaussians[i][j] = make([]float32, dimensions)
			for k := uint32(0); k < dimensions; k++ {
				gaussians[i][j][k], err = readFloat32(r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	s := make([][][]float32, repetitions)
	for i := uint32(0); i < repetitions; i++ {
		s[i] = make([][]float32, dProjections)
		for j := uint32(0); j < dProjections; j++ {
			s[i][j] = make([]float32, dimensions)
			for k := uint32(0); k < dimensions; k++ {
				s[i][j][k], err = readFloat32(r)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	mv := multivector.MuveraData{
		KSim:         kSim,
		NumClusters:  numClusters,
		Dimensions:   dimensions,
		DProjections: dProjections,
		Repetitions:  repetitions,
		Gaussians:    gaussians,
		S:            s,
	}
	return &mv, nil
}

// ---------------------------------------------------------------------------
// small wrappers for compression commits
// ---------------------------------------------------------------------------

func (w *WALCommitReader) readAddPQ() (Commit, error) {
	data, err := readPQData(w.r)
	if err != nil {
		return nil, err
	}
	return &AddPQCommit{Data: data}, nil
}

func (w *WALCommitReader) readAddSQ() (Commit, error) {
	data, err := readSQData(w.r)
	if err != nil {
		return nil, err
	}
	return &AddSQCommit{Data: data}, nil
}

func (w *WALCommitReader) readAddRQ() (Commit, error) {
	data, err := readRQData(w.r)
	if err != nil {
		return nil, err
	}
	return &AddRQCommit{Data: data}, nil
}

func (w *WALCommitReader) readAddBRQ() (Commit, error) {
	data, err := readBRQData(w.r)
	if err != nil {
		return nil, err
	}
	return &AddBRQCommit{Data: data}, nil
}

func (w *WALCommitReader) readAddMuvera() (Commit, error) {
	data, err := readMuveraData(w.r)
	if err != nil {
		return nil, err
	}
	return &AddMuveraCommit{Data: data}, nil
}
