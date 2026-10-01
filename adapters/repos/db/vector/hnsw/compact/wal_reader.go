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

package compact

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

// Commit represents a single HNSW graph operation read from a WAL file.
//
// Commits are categorized into two groups:
//
// Global commits (no node ID, apply to entire index):
//   - [SetEntryPointMaxLevelCommit] - Sets graph entrypoint and max level
//   - [ResetIndexCommit] - Clears entire index
//   - [AddPQCommit], [AddSQCommit], [AddRQCommit], [AddBRQCommit] - Compression data
//   - [AddMuveraCommit] - Multi-vector encoder data
//
// Node-specific commits (have a node ID):
//   - [AddNodeCommit] - Creates a node with a level
//   - [DeleteNodeCommit] - Removes a node
//   - [AddLinkAtLevelCommit], [AddLinksAtLevelCommit] - Add connections
//   - [ReplaceLinksAtLevelCommit] - Replace all connections at a level
//   - [ClearLinksCommit], [ClearLinksAtLevelCommit] - Remove connections
//   - [AddTombstoneCommit], [RemoveTombstoneCommit] - Tombstone management
//
// Use [WALCommitReader] to stream commits from a file, or [Iterator] for
// node-grouped iteration suitable for merging.
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

// countingReader wraps an io.Reader and tracks bytes read.
// Used for tracking valid file position for truncation after crash recovery.
type countingReader struct {
	r     io.Reader
	count int64
}

func (c *countingReader) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	c.count += int64(n)
	return n, err
}

// WALCommitReader streams [Commit] values from a WAL file (raw, .condensed,
// or .sorted format). It handles the low-level binary deserialization but
// does not apply commits to any in-memory state.
//
// For building in-memory graph state, wrap with [InMemoryReader].
// For node-grouped iteration suitable for merging, wrap with [Iterator].
//
// The reader is stateful and reads commits sequentially. Call
// [WALCommitReader.ReadNextCommit] repeatedly until io.EOF.
type WALCommitReader struct {
	r         *bufio.Reader
	countingR *countingReader // tracks bytes read from underlying reader
	logger    logrus.FieldLogger

	// lastValidOffset is the byte offset at the end of the last fully decoded
	// commit. Unlike BytesRead, it never includes the partially-consumed bytes
	// of a failed read, so it is the correct point to truncate a corrupt file
	// back to its last valid record.
	lastValidOffset int64

	// layout, when set, rejects records that cannot appear at their position
	// in a compacted segment. See NewWALCommitReaderForFile.
	layout *compactedLayout

	// maxNodeID, when non-zero, rejects records naming a higher node ID.
	maxNodeID uint64

	reusableBuf     []byte
	reusableUint64s []uint64
}

// NewWALCommitReader wraps an io.Reader. Caller controls flow by repeatedly
// calling ReadNextCommit().
func NewWALCommitReader(r io.Reader, logger logrus.FieldLogger) *WALCommitReader {
	cr := &countingReader{r: r}
	return &WALCommitReader{
		r:         bufio.NewReader(cr),
		countingR: cr,
		logger:    logger,
	}
}

// NewWALCommitReaderForFile is NewWALCommitReader with the record-layout check
// of fileType enabled: a .sorted or .condensed segment has no checksum, so a
// record the compaction writers never put at that position is reported as an
// error instead of being returned, and LastValidOffset stays before it.
func NewWALCommitReaderForFile(r io.Reader, fileType FileType, logger logrus.FieldLogger) *WALCommitReader {
	w := NewWALCommitReader(r, logger)
	if fileType == FileTypeSorted || fileType == FileTypeCondensed {
		w.layout = &compactedLayout{fileType: fileType}
	}
	return w
}

// errNodeIDBeyondLimit reports a record naming a node ID above the index's
// limit, which only corruption produces.
var errNodeIDBeyondLimit = errors.New("node ID beyond the index's limit")

// limitNodeIDs makes ReadNextCommit reject records naming a node ID above limit,
// leaving LastValidOffset before them. 0 means no limit.
func (w *WALCommitReader) limitNodeIDs(limit uint64) *WALCommitReader {
	w.maxNodeID = limit
	return w
}

// highestNodeID returns the highest node ID a commit names, if any.
func highestNodeID(c Commit) (uint64, bool) {
	switch ct := c.(type) {
	case *AddNodeCommit:
		return ct.ID, true
	case *SetEntryPointMaxLevelCommit:
		return ct.Entrypoint, true
	case *AddLinkAtLevelCommit:
		return max(ct.Source, ct.Target), true
	case *AddLinksAtLevelCommit:
		return maxOf(ct.Source, ct.Targets), true
	case *ReplaceLinksAtLevelCommit:
		return maxOf(ct.Source, ct.Targets), true
	case *AddTombstoneCommit:
		return ct.ID, true
	case *RemoveTombstoneCommit:
		return ct.ID, true
	case *ClearLinksCommit:
		return ct.ID, true
	case *ClearLinksAtLevelCommit:
		return ct.ID, true
	case *DeleteNodeCommit:
		return ct.ID, true
	default:
		return 0, false
	}
}

func maxOf(id uint64, ids []uint64) uint64 {
	for _, x := range ids {
		id = max(id, x)
	}
	return id
}

// BytesRead returns the number of bytes successfully read from the underlying
// reader, accounting for buffered but unprocessed data. This is used to determine
// the valid file position for truncation after detecting corrupt/truncated data.
func (w *WALCommitReader) BytesRead() int64 {
	return w.countingR.count - int64(w.r.Buffered())
}

// LastValidOffset returns the byte offset at the end of the last commit that
// was decoded successfully. Use this (not BytesRead) to truncate a file back to
// its last valid record after a read error: BytesRead includes the type byte
// and any partial body consumed by the failed read, so truncating to it would
// leave a stray fragment that re-triggers the error on the next read.
func (w *WALCommitReader) LastValidOffset() int64 {
	return w.lastValidOffset
}

// ReadNextCommit returns the next commit in the WAL.
//   - (Commit, nil) on success
//   - (nil, io.EOF) when no more commits are available
//   - (nil, err) on other errors
//
// A record cut off after its type byte is reported as io.ErrUnexpectedEOF, never
// io.EOF. On success it advances LastValidOffset to the end of the returned commit.
func (w *WALCommitReader) ReadNextCommit() (Commit, error) {
	c, err := w.decodeNextCommit()
	if err != nil {
		return nil, err
	}
	if w.layout != nil {
		if err := w.layout.check(c); err != nil {
			return nil, err
		}
	}
	if w.maxNodeID > 0 {
		if id, ok := highestNodeID(c); ok && id > w.maxNodeID {
			return nil, errors.Wrapf(errNodeIDBeyondLimit, "%s record names node %d, limit %d", c.Type(), id, w.maxNodeID)
		}
	}
	w.lastValidOffset = w.BytesRead()
	return c, nil
}

func (w *WALCommitReader) decodeNextCommit() (Commit, error) {
	ct, err := readCommitType(w.r)
	if err != nil {
		return nil, err
	}

	c, err := w.decodeCommitBody(ct)
	if errors.Is(err, io.EOF) {
		// A body field hit EOF with zero bytes left: the log ends between two
		// fields of this commit, which is a torn tail, not a clean end.
		return nil, errors.Wrapf(io.ErrUnexpectedEOF, "commit type %d: %v", ct, err)
	}
	return c, err
}

func (w *WALCommitReader) decodeCommitBody(ct HnswCommitType) (Commit, error) {
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
	case AddRQCentered:
		return w.readAddRQCentered()
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
	// Writers only produce segments that divide the dimensions and a non-zero
	// centroid count. A record without segments carries no encoders, and one
	// without centroids restores encoders with no centers.
	if m == 0 || ks == 0 || dims%m != 0 {
		return nil, errors.Errorf("pq with %d dimensions, %d segments and %d centroids", dims, m, ks)
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
		// Zero-width segments hold no bytes, so m and ks would not be bounded
		// by the bytes read. Real PQ segments divide the dimensions.
		if dims/m == 0 {
			return nil, errors.Errorf("pq with %d dimensions cannot have %d segments", dims, m)
		}
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

// maxPreallocEntries caps the capacity taken from a count in a record header.
// Headers are not checksummed, so slices grow as their elements are actually
// read and garbage cannot request more memory than the bytes behind it.
const maxPreallocEntries = 4096

func preallocCap(n uint32) int {
	return int(min(n, maxPreallocEntries))
}

func readFloat32s(r io.Reader, n uint32) ([]float32, error) {
	out := make([]float32, 0, preallocCap(n))
	for i := uint32(0); i < n; i++ {
		f, err := readFloat32(r)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

// readRotation reads the swaps and signs of a FastRotation.
func readRotation(r io.Reader, outputDim, rounds uint32) ([][]compression.Swap, [][]float32, error) {
	// Real rotations have an output dimension of at least 64. Below 2 a round
	// holds no swaps, so rounds would not be bounded by the bytes read.
	if rounds > 0 && outputDim < 2 {
		return nil, nil, errors.Errorf("rotation with %d rounds has output dimension %d", rounds, outputDim)
	}

	swaps := make([][]compression.Swap, 0, preallocCap(rounds))
	for i := uint32(0); i < rounds; i++ {
		round := make([]compression.Swap, 0, preallocCap(outputDim/2))
		for j := uint32(0); j < outputDim/2; j++ {
			var s compression.Swap
			var err error
			s.I, err = readUint16(r)
			if err != nil {
				return nil, nil, err
			}
			s.J, err = readUint16(r)
			if err != nil {
				return nil, nil, err
			}
			round = append(round, s)
		}
		swaps = append(swaps, round)
	}

	signs := make([][]float32, 0, preallocCap(rounds))
	for i := uint32(0); i < rounds; i++ {
		round, err := readFloat32s(r, outputDim)
		if err != nil {
			return nil, nil, err
		}
		signs = append(signs, round)
	}

	return swaps, signs, nil
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

	swaps, signs, err := readRotation(r, outputDim, rounds)
	if err != nil {
		return nil, err
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

	swaps, signs, err := readRotation(r, outputDim, rounds)
	if err != nil {
		return nil, err
	}

	rounding, err := readFloat32s(r, outputDim)
	if err != nil {
		return nil, err
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

	if repetitions > 0 && (dimensions == 0 || (kSim == 0 && dProjections == 0)) {
		return nil, errors.Errorf("muvera encoder with %d repetitions has no payload", repetitions)
	}

	readMatrices := func(rows uint32) ([][][]float32, error) {
		out := make([][][]float32, 0, preallocCap(repetitions))
		for i := uint32(0); i < repetitions; i++ {
			m := make([][]float32, 0, preallocCap(rows))
			for j := uint32(0); j < rows; j++ {
				v, err := readFloat32s(r, dimensions)
				if err != nil {
					return nil, err
				}
				m = append(m, v)
			}
			out = append(out, m)
		}
		return out, nil
	}

	var gaussians [][][]float32
	if kSim > 0 {
		gaussians, err = readMatrices(kSim)
		if err != nil {
			return nil, err
		}
	}

	s, err := readMatrices(dProjections)
	if err != nil {
		return nil, err
	}

	if kSim == 0 {
		// Zero-width gaussians occupy no bytes, so they are only built once S
		// has shown that repetitions is backed by data.
		gaussians = make([][][]float32, repetitions)
		for i := range gaussians {
			gaussians[i] = make([][]float32, 0)
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

func (w *WALCommitReader) readAddRQCentered() (Commit, error) {
	flags, err := readByte(w.r)
	if err != nil {
		return nil, err
	}
	data, err := readRQData(w.r)
	if err != nil {
		return nil, err
	}
	if err := applyRQCenteredFlags(data, flags); err != nil {
		return nil, err
	}
	meanLen, err := readUint32(w.r)
	if err != nil {
		return nil, err
	}
	// The mean always has exactly InputDim entries; a mismatch surfaces the
	// corruption here instead of at restore.
	if meanLen != data.InputDim {
		return nil, errors.Errorf("centered RQ mean length %d does not match input dimension %d", meanLen, data.InputDim)
	}
	mean, err := readFloat32s(w.r, meanLen)
	if err != nil {
		return nil, err
	}
	data.Mean = mean
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
