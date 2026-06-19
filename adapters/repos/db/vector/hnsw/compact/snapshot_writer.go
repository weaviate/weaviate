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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

const (
	// Snapshot version constants
	snapshotVersionV1 = 1
	snapshotVersionV2 = 2
	snapshotVersionV3 = 3

	// defaultBlockSize is the default size of each block in the snapshot body (4MB).
	defaultBlockSize = 4 * 1024 * 1024
)

// Snapshot compression type constants (must match the hnsw package values)
const (
	SnapshotCompressionTypePQ  = 1
	SnapshotCompressionTypeSQ  = 2
	SnapshotEncoderTypeMuvera  = 3 // Note: Muvera is an encoder, not compression
	SnapshotCompressionTypeRQ  = 4
	SnapshotCompressionTypeBRQ = 5
)

// SnapshotWriter writes HNSW state to the V3 snapshot format.
// It converts commit-based data from the merger into absolute state snapshots.
//
// The V3 snapshot format consists of:
//   - Metadata header: version, checksum, metadata size, and metadata content
//   - Body: fixed-size blocks (4MB each) containing node data with checksums
//
// Node data is streamed: as each node arrives (in ascending ID order) it is
// encoded straight into the current body block and full blocks are spilled to
// a scratch file. The writer never holds the whole graph, so peak heap is
// bounded by one block plus the largest single node, independent of the
// live-set size and of maxNodeID. The metadata header is written to w only
// once the node count is known (after the body has been spilled), then the
// spilled body is copied in behind it. See bodyStreamer.
type SnapshotWriter struct {
	w         io.Writer
	blockSize int64
	logger    logrus.FieldLogger

	// scratchDir is where the body spill file is created. Empty means the OS
	// temp dir. Production sets this to the index directory (a data volume) so
	// a multi-GB body never lands on a small tmpfs. See WithScratchDir.
	scratchDir string

	// Metadata collected before writing
	entrypoint uint64
	level      uint16

	// body streams encoded node blocks to a scratch file; created lazily on
	// the first node and torn down by Flush.
	body         *bodyStreamer
	nodeEntryBuf bytes.Buffer // reused scratch for encoding a single node entry

	// firstErr latches the first error from the no-return AddNode/AddTombstone
	// API so it surfaces from Flush, preserving the pre-streaming contract.
	firstErr error

	// Compression data (only one can be set at a time)
	pqData  *compression.PQData
	sqData  *compression.SQData
	rqData  *compression.RQData
	brqData *compression.BRQData

	// Muvera encoder data (can be set alongside compression)
	muveraData *multivector.MuveraData

	// shouldAbort, if non-nil, is polled at coarse intervals during
	// WriteFromMerger so an in-progress snapshot can yield to a Drop or
	// Shutdown without running the merger to exhaustion. See
	// ErrCompactionAborted for the contract.
	shouldAbort func() bool
}

// NewSnapshotWriter creates a new snapshot writer.
func NewSnapshotWriter(w io.Writer) *SnapshotWriter {
	return &SnapshotWriter{
		w:         w,
		blockSize: defaultBlockSize,
	}
}

// NewSnapshotWriterWithBlockSize creates a new snapshot writer with a custom block size.
// This is primarily useful for testing with smaller block sizes.
func NewSnapshotWriterWithBlockSize(w io.Writer, blockSize int64) *SnapshotWriter {
	return &SnapshotWriter{
		w:         w,
		blockSize: blockSize,
	}
}

// WithScratchDir sets the directory for the body spill file. It should be on
// the same (large) volume as the snapshot output; the default OS temp dir may
// be a small tmpfs that a large body would exhaust.
func (s *SnapshotWriter) WithScratchDir(dir string) *SnapshotWriter {
	s.scratchDir = dir
	return s
}

// WithLogger sets the logger for the snapshot writer. If set, corrupt nodes
// encountered during compaction are logged before being skipped.
func (s *SnapshotWriter) WithLogger(logger logrus.FieldLogger) *SnapshotWriter {
	s.logger = logger
	return s
}

// WithAbort installs a poll callback that WriteFromMerger consults before
// reading each merged node. If the callback returns true the writer
// returns ErrCompactionAborted and leaves no committed file behind (the
// caller's SafeFileWriter Abort() runs via defer).
func (s *SnapshotWriter) WithAbort(shouldAbort func() bool) *SnapshotWriter {
	s.shouldAbort = shouldAbort
	return s
}

// SetEntrypoint sets the entrypoint and max level for the snapshot.
func (s *SnapshotWriter) SetEntrypoint(entrypoint uint64, level uint16) {
	s.entrypoint = entrypoint
	s.level = level
}

// SetPQData sets the Product Quantization compression data.
func (s *SnapshotWriter) SetPQData(data *compression.PQData) {
	s.pqData = data
}

// SetSQData sets the Scalar Quantization compression data.
func (s *SnapshotWriter) SetSQData(data *compression.SQData) {
	s.sqData = data
}

// SetRQData sets the Rotational Quantization compression data.
func (s *SnapshotWriter) SetRQData(data *compression.RQData) {
	s.rqData = data
}

// SetBRQData sets the Binary Rotational Quantization compression data.
func (s *SnapshotWriter) SetBRQData(data *compression.BRQData) {
	s.brqData = data
}

// SetMuveraData sets the Muvera encoder data.
func (s *SnapshotWriter) SetMuveraData(data *multivector.MuveraData) {
	s.muveraData = data
}

// isCompressed returns true if any compression data is set.
func (s *SnapshotWriter) isCompressed() bool {
	return s.pqData != nil || s.sqData != nil || s.rqData != nil || s.brqData != nil
}

// isMuveraEnabled returns true if Muvera encoder data is set.
func (s *SnapshotWriter) isMuveraEnabled() bool {
	return s.muveraData != nil
}

// AddNode adds a node with its connections to the snapshot.
// Nodes must be added in ascending node ID order.
//
// The node is encoded and spilled to the body immediately; any error is
// latched and surfaced from Flush, preserving the original no-error signature.
func (s *SnapshotWriter) AddNode(nodeID uint64, level uint16, connections [][]uint64, hasTombstone bool) {
	if s.firstErr != nil {
		return
	}
	if err := s.addNodeStreaming(nodeID, level, connections, hasTombstone); err != nil {
		s.firstErr = err
	}
}

// addNodeStreaming encodes one node entry and hands it to the body streamer.
// The encode buffer is reused across calls so the per-node heap cost is just
// the node's own connection data, not retained after the block absorbs it.
func (s *SnapshotWriter) addNodeStreaming(nodeID uint64, level uint16, connections [][]uint64, hasTombstone bool) error {
	if err := s.ensureBody(); err != nil {
		return err
	}

	s.nodeEntryBuf.Reset()
	if hasTombstone {
		_ = writeByte(&s.nodeEntryBuf, 1) // exists with tombstone
	} else {
		_ = writeByte(&s.nodeEntryBuf, 2) // exists without tombstone
	}
	_ = writeUint32(&s.nodeEntryBuf, uint32(level))

	connData, err := s.packConnections(level, connections)
	if err != nil {
		return errors.Wrapf(err, "pack connections for node %d", nodeID)
	}
	_ = writeUint32(&s.nodeEntryBuf, uint32(len(connData)))
	_, _ = s.nodeEntryBuf.Write(connData)

	return s.body.addLiveNode(nodeID, s.nodeEntryBuf.Bytes())
}

// AddTombstone marks a standalone tombstone for a nil slot.
//
// This does NOT emit a tombstone entry to the body: the V3 format has only
// three existence bytes (0=absent, 1=alive-with-tombstone,
// 2=alive-without-tombstone) and no encoding for a "deleted-with-tombstone"
// slot. Its visible effect is sizing: it extends the node count so the
// metadata covers nodeID, and bodyStreamer.finish emits absent markers through
// that reserved range. Use AddNode with hasTombstone=true to persist a
// tombstone for an alive node.
func (s *SnapshotWriter) AddTombstone(nodeID uint64) {
	if s.firstErr != nil {
		return
	}
	if err := s.ensureBody(); err != nil {
		s.firstErr = err
		return
	}
	s.body.reserve(nodeID)
}

// ensureBody lazily opens the body spill file on the first node.
func (s *SnapshotWriter) ensureBody() error {
	if s.body != nil {
		return nil
	}
	b, err := newBodyStreamer(s.scratchDir, s.blockSize)
	if err != nil {
		return err
	}
	s.body = b
	return nil
}

// Flush finalizes the body, writes the metadata header (now that the node
// count is known), then copies the spilled body in behind it. The spill file
// is always removed, including on the error paths.
func (s *SnapshotWriter) Flush() error {
	if s.body != nil {
		defer s.body.close()
	}
	if s.firstErr != nil {
		return s.firstErr
	}

	var nodeCount uint32
	if s.body != nil {
		if err := s.body.finish(); err != nil {
			return errors.Wrap(err, "finalize snapshot body")
		}
		nodeCount = uint32(s.body.count)
	}

	if err := s.writeMetadata(nodeCount); err != nil {
		return errors.Wrap(err, "write metadata")
	}

	if s.body != nil && nodeCount > 0 {
		if err := s.body.copyTo(s.w); err != nil {
			return errors.Wrap(err, "write body")
		}
	}

	return nil
}

// writeMetadata writes the snapshot metadata header. nodeCount is the total
// slot count (maxNodeID+1), sized so the reader can pre-allocate Graph.Nodes.
func (s *SnapshotWriter) writeMetadata(nodeCount uint32) error {
	var buf bytes.Buffer

	// Metadata content
	_ = writeUint64(&buf, s.entrypoint)
	_ = writeUint16(&buf, s.level)

	// Write isCompressed flag and compression data
	isCompressed := s.isCompressed()
	_ = writeBool(&buf, isCompressed)

	if isCompressed {
		if err := s.writeCompressionData(&buf); err != nil {
			return errors.Wrap(err, "write compression data")
		}
	}

	// Write isEncoded flag and Muvera data
	isMuveraEnabled := s.isMuveraEnabled()
	_ = writeBool(&buf, isMuveraEnabled)

	if isMuveraEnabled {
		if err := s.writeMuveraData(&buf); err != nil {
			return errors.Wrap(err, "write muvera data")
		}
	}

	// Node count
	_ = writeUint32(&buf, nodeCount)

	// Compute checksum of metadata
	metadataSize := uint32(buf.Len())

	hasher := crc32.NewIEEE()
	// Write version byte to hasher
	var versionBuf [1]byte
	versionBuf[0] = byte(snapshotVersionV3)
	_, _ = hasher.Write(versionBuf[:])
	// Write metadata size to hasher
	var metaSizeBuf [4]byte
	binary.LittleEndian.PutUint32(metaSizeBuf[:], metadataSize)
	_, _ = hasher.Write(metaSizeBuf[:])
	_, _ = hasher.Write(buf.Bytes())

	// Write header: version | checksum | metadataSize | metadata
	if err := writeByte(s.w, snapshotVersionV3); err != nil {
		return err
	}
	// Write checksum
	var checksumBuf [4]byte
	binary.LittleEndian.PutUint32(checksumBuf[:], hasher.Sum32())
	if _, err := s.w.Write(checksumBuf[:]); err != nil {
		return err
	}
	// Write metadata size
	if _, err := s.w.Write(metaSizeBuf[:]); err != nil {
		return err
	}
	if _, err := s.w.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// writeCompressionData writes compression data to the buffer.
// The format matches the old snapshot implementation for compatibility.
func (s *SnapshotWriter) writeCompressionData(buf *bytes.Buffer) error {
	if s.pqData != nil {
		return s.writePQData(buf)
	}
	if s.sqData != nil {
		return s.writeSQData(buf)
	}
	if s.rqData != nil {
		return s.writeRQData(buf)
	}
	if s.brqData != nil {
		return s.writeBRQData(buf)
	}
	return nil
}

// writePQData writes Product Quantization data to the buffer.
func (s *SnapshotWriter) writePQData(buf *bytes.Buffer) error {
	data := s.pqData
	_ = writeByte(buf, byte(SnapshotCompressionTypePQ))
	_ = writeUint16(buf, data.Dimensions)
	_ = writeUint16(buf, data.Ks)
	_ = writeUint16(buf, data.M)
	_ = writeByte(buf, byte(data.EncoderType))
	_ = writeByte(buf, data.EncoderDistribution)
	_ = writeBool(buf, data.UseBitsEncoding)

	for _, encoder := range data.Encoders {
		_, _ = buf.Write(encoder.ExposeDataForRestore())
	}
	return nil
}

// writeSQData writes Scalar Quantization data to the buffer.
func (s *SnapshotWriter) writeSQData(buf *bytes.Buffer) error {
	data := s.sqData
	_ = writeByte(buf, byte(SnapshotCompressionTypeSQ))
	_ = writeUint16(buf, data.Dimensions)
	_ = writeUint32(buf, math.Float32bits(data.A))
	_ = writeUint32(buf, math.Float32bits(data.B))
	return nil
}

// writeRQData writes Rotational Quantization data to the buffer.
func (s *SnapshotWriter) writeRQData(buf *bytes.Buffer) error {
	data := s.rqData
	_ = writeByte(buf, byte(SnapshotCompressionTypeRQ))
	_ = writeUint32(buf, data.InputDim)
	_ = writeUint32(buf, data.Bits)
	_ = writeUint32(buf, data.Rotation.OutputDim)
	_ = writeUint32(buf, data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			_ = writeUint16(buf, dim.I)
			_ = writeUint16(buf, dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		for _, dim := range sign {
			_ = writeFloat32(buf, dim)
		}
	}
	return nil
}

// writeBRQData writes Binary Rotational Quantization data to the buffer.
func (s *SnapshotWriter) writeBRQData(buf *bytes.Buffer) error {
	data := s.brqData
	_ = writeByte(buf, byte(SnapshotCompressionTypeBRQ))
	_ = writeUint32(buf, data.InputDim)
	_ = writeUint32(buf, data.Rotation.OutputDim)
	_ = writeUint32(buf, data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			_ = writeUint16(buf, dim.I)
			_ = writeUint16(buf, dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		for _, dim := range sign {
			_ = writeFloat32(buf, dim)
		}
	}

	for _, rounding := range data.Rounding {
		_ = writeFloat32(buf, rounding)
	}
	return nil
}

// writeMuveraData writes Muvera encoder data to the buffer.
func (s *SnapshotWriter) writeMuveraData(buf *bytes.Buffer) error {
	data := s.muveraData
	_ = writeByte(buf, byte(SnapshotEncoderTypeMuvera))
	_ = writeUint32(buf, data.Dimensions)
	_ = writeUint32(buf, data.KSim)
	_ = writeUint32(buf, data.NumClusters)
	_ = writeUint32(buf, data.DProjections)
	_ = writeUint32(buf, data.Repetitions)

	for _, gaussian := range data.Gaussians {
		for _, cluster := range gaussian {
			for _, el := range cluster {
				_ = writeUint32(buf, math.Float32bits(el))
			}
		}
	}

	for _, matrix := range data.S {
		for _, vector := range matrix {
			for _, el := range vector {
				_ = writeUint32(buf, math.Float32bits(el))
			}
		}
	}
	return nil
}

// absentSlot is the single-byte body entry for a slot with no live node
// (existence == 0). It is never mutated.
var absentSlot = []byte{0}

// bodyStreamer encodes nodes into the V3 body block format and spills full
// blocks to a scratch file as they fill, so peak heap stays bounded by one
// block plus the largest single node entry — independent of the live-set size
// and of maxNodeID. The byte stream it produces is identical to materializing
// the whole graph and writing it slot-by-slot: every slot from 0 to the max
// reserved ID gets exactly one entry (a full node or a one-byte absent
// marker), each fixed-size block is self-describing (leading start-node-ID,
// trailing length, leading CRC), and gaps are absent markers so the reader's
// per-slot counter stays aligned with node IDs.
type bodyStreamer struct {
	scratch      *os.File
	scratchPath  string
	blockSize    int64
	maxBlockSize int

	block   bytes.Buffer
	hasher  hash.Hash32
	hw      io.Writer // MultiWriter(&block, hasher) for the current block
	zeroPad []byte    // reusable maxBlockSize-sized zero buffer for padding

	started bool   // current block has been initialized with a start-node-ID
	nextID  uint64 // next slot ID expected by the block stream
	count   uint64 // total slots reserved (= maxNodeID+1); the metadata node count
}

// newBodyStreamer creates the scratch spill file. scratchDir empty means the
// OS temp dir. The file is O_RDWR (created via CreateTemp) so it can be read
// back when copied behind the header. It is always removed via close.
func newBodyStreamer(scratchDir string, blockSize int64) (*bodyStreamer, error) {
	f, err := os.CreateTemp(scratchDir, "hnsw-snapshot-body-*.tmp")
	if err != nil {
		return nil, errors.Wrap(err, "create snapshot body scratch file")
	}
	maxBlockSize := int(blockSize - 8) // reserve 8 bytes for checksum and block length
	return &bodyStreamer{
		scratch:      f,
		scratchPath:  f.Name(),
		blockSize:    blockSize,
		maxBlockSize: maxBlockSize,
		hasher:       crc32.NewIEEE(),
		zeroPad:      make([]byte, maxBlockSize),
	}, nil
}

// reserve grows the node count so the metadata covers id, without emitting a
// live node — used for standalone tombstones on nil slots.
func (b *bodyStreamer) reserve(id uint64) {
	if id+1 > b.count {
		b.count = id + 1
	}
}

// addLiveNode writes the encoded entry for a live node at id, first emitting
// absent markers for any skipped slots so the on-disk slot index stays aligned
// with node IDs. entry is consumed synchronously (copied into the block), so
// the caller may reuse its backing buffer afterwards.
func (b *bodyStreamer) addLiveNode(id uint64, entry []byte) error {
	if id < b.nextID {
		return errors.Errorf("nodes must be added in ascending ID order: got %d after %d", id, b.nextID)
	}
	for ; b.nextID < id; b.nextID++ {
		if err := b.writeSlot(b.nextID, absentSlot); err != nil {
			return err
		}
	}
	if err := b.writeSlot(id, entry); err != nil {
		return err
	}
	b.nextID = id + 1
	b.reserve(id)
	return nil
}

// writeSlot appends one slot entry to the current block, flushing and starting
// a fresh block (keyed on slotID) when the entry would not fit.
func (b *bodyStreamer) writeSlot(slotID uint64, entry []byte) error {
	if !b.started {
		// The first block always starts at slot 0; gap fills begin at nextID==0.
		if err := b.initBlock(0); err != nil {
			return err
		}
		b.started = true
	}

	// A single entry that cannot fit even an otherwise-empty block (8-byte
	// start-node-ID header) would overflow flushBlock's padding. This only
	// occurs for corrupt nodes with absurd connection counts; fail loudly
	// rather than emit a malformed (oversized) block.
	if len(entry)+8 > b.maxBlockSize {
		return errors.Errorf("node %d entry of %d bytes exceeds block capacity %d", slotID, len(entry), b.maxBlockSize-8)
	}

	if len(entry)+b.block.Len() < b.maxBlockSize {
		_, err := b.hw.Write(entry)
		return err
	}

	if err := b.flushBlock(); err != nil {
		return err
	}
	if err := b.initBlock(slotID); err != nil {
		return err
	}
	_, err := b.hw.Write(entry)
	return err
}

// initBlock resets the block buffer and hasher and writes the start-node-ID
// header for the next block.
func (b *bodyStreamer) initBlock(startID uint64) error {
	b.block.Reset()
	b.hasher.Reset()
	b.hw = io.MultiWriter(&b.block, b.hasher)
	return writeUint64(b.hw, startID)
}

// flushBlock pads the current block to maxBlockSize, appends the block length
// and CRC, and writes the complete fixed-size block to the scratch file.
func (b *bodyStreamer) flushBlock() error {
	blockLen := b.block.Len()

	if pad := b.maxBlockSize - blockLen; pad > 0 {
		_, _ = b.block.Write(b.zeroPad[:pad])
		_, _ = b.hasher.Write(b.zeroPad[:pad])
	}

	// Block length goes at the end of the block and into the hash.
	if err := writeUint32(&b.block, uint32(blockLen)); err != nil {
		return err
	}
	var blockLenBuf [4]byte
	binary.LittleEndian.PutUint32(blockLenBuf[:], uint32(blockLen))
	_, _ = b.hasher.Write(blockLenBuf[:])

	// Checksum first, then the padded block + length.
	checksum := b.hasher.Sum32()
	if err := writeUint32(b.scratch, checksum); err != nil {
		return err
	}
	if _, err := b.scratch.Write(b.block.Bytes()); err != nil {
		return err
	}
	return nil
}

// finish emits absent markers for any reserved-but-unwritten trailing slots
// (e.g. tombstones past the last live node) and flushes the final block.
func (b *bodyStreamer) finish() error {
	for ; b.nextID < b.count; b.nextID++ {
		if err := b.writeSlot(b.nextID, absentSlot); err != nil {
			return err
		}
	}
	if b.started && b.block.Len() > 0 {
		if err := b.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// copyTo streams the spilled body from the start of the scratch file to w
// using a bounded buffer.
func (b *bodyStreamer) copyTo(w io.Writer) error {
	if _, err := b.scratch.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek body scratch file")
	}
	if _, err := io.Copy(w, b.scratch); err != nil {
		return errors.Wrap(err, "copy body from scratch file")
	}
	return nil
}

// close removes the scratch file. It is idempotent so it can be deferred on
// every path (Flush, abort, error) without double-free concerns.
func (b *bodyStreamer) close() {
	if b.scratch == nil {
		return
	}
	name := b.scratchPath
	_ = b.scratch.Close()
	_ = os.Remove(name)
	b.scratch = nil
}

// packConnections converts connections to the packed binary format.
// This uses the packedconn package to ensure compatibility with the snapshot reader.
func (s *SnapshotWriter) packConnections(level uint16, connections [][]uint64) ([]byte, error) {
	if len(connections) == 0 {
		return nil, nil
	}

	if len(connections) > packedconn.MaxLayerCount {
		return nil, fmt.Errorf("connection layers (%d) exceed maximum (%d) for node with level %d",
			len(connections), packedconn.MaxLayerCount, level)
	}

	// Use packedconn to create properly formatted connection data
	pc, err := packedconn.NewWithElements(connections)
	if err != nil {
		return nil, errors.Wrapf(err, "create packed connections for node with level %d", level)
	}

	return pc.Data(), nil
}

// WriteFromMerger writes snapshot data from an n-way merger.
// This converts commit-based data to absolute state and streams it to the
// snapshot. Each node is encoded and spilled as it arrives; the writer never
// holds the whole graph.
func (s *SnapshotWriter) WriteFromMerger(merger *NWayMerger) error {
	// The body spill file is removed on every exit path. close is idempotent,
	// so the successful path (Flush also closes) and the abort/error paths
	// (which return before Flush) all leave no scratch file behind.
	defer func() {
		if s.body != nil {
			s.body.close()
		}
	}()

	// Extract global state from merged commits
	for _, c := range merger.GlobalCommits() {
		switch ct := c.(type) {
		case *SetEntryPointMaxLevelCommit:
			s.SetEntrypoint(ct.Entrypoint, ct.Level)
		case *AddPQCommit:
			s.SetPQData(ct.Data)
		case *AddSQCommit:
			s.SetSQData(ct.Data)
		case *AddRQCommit:
			s.SetRQData(ct.Data)
		case *AddBRQCommit:
			s.SetBRQData(ct.Data)
		case *AddMuveraCommit:
			s.SetMuveraData(ct.Data)
		}
	}

	// Process all nodes from the merger
	for {
		if s.shouldAbort != nil && s.shouldAbort() {
			return ErrCompactionAborted
		}
		nodeCommits, err := merger.Next()
		if err != nil {
			return errors.Wrap(err, "read next node from merger")
		}
		if nodeCommits == nil {
			// No more nodes
			break
		}

		// Convert commits to absolute state. A nil return means the node
		// is deleted (commitsToNodeState's only nil-return path) and is
		// therefore absent from the snapshot's absolute state. Any
		// AddTombstoneCommit paired with a DeleteNodeCommit is dropped
		// here deliberately — see the absent-slot handling in bodyStreamer
		// for the invariant that makes this safe.
		state := s.commitsToNodeState(nodeCommits)
		if state != nil {
			if err := s.addNodeStreaming(nodeCommits.NodeID, state.level, state.connections, state.hasTombstone); err != nil {
				return errors.Wrapf(err, "add node %d", nodeCommits.NodeID)
			}
		}
	}

	return s.Flush()
}

// commitsToNodeState converts a set of commits for a node into absolute state.
// Returns nil if the node was deleted or has no meaningful state.
func (s *SnapshotWriter) commitsToNodeState(nc *NodeCommits) *nodeStateWithTombstone {
	var level uint16
	var hasLevel bool
	connections := make(map[uint16][]uint64)
	deleted := false
	hasTombstone := false

	for _, c := range nc.Commits {
		switch ct := c.(type) {
		case *DeleteNodeCommit:
			deleted = true

		case *AddNodeCommit:
			if !deleted {
				level = ct.Level
				hasLevel = true
			}

		case *AddTombstoneCommit:
			hasTombstone = true

		case *RemoveTombstoneCommit:
			hasTombstone = false

		case *ReplaceLinksAtLevelCommit:
			if !deleted {
				connections[ct.Level] = ct.Targets
			}

		case *AddLinksAtLevelCommit:
			if !deleted {
				existing := connections[ct.Level]
				connections[ct.Level] = append(existing, ct.Targets...)
			}

		case *AddLinkAtLevelCommit:
			if !deleted {
				existing := connections[ct.Level]
				connections[ct.Level] = append(existing, ct.Target)
			}

		case *ClearLinksAtLevelCommit:
			if !deleted {
				connections[ct.Level] = nil
			}

		case *ClearLinksCommit:
			if !deleted {
				connections = make(map[uint16][]uint64)
			}
		}
	}

	// If node was deleted, return nil (no node data to write)
	if deleted {
		return nil
	}

	// If we have no level and no connections, this might be a tombstone-only node
	if !hasLevel && len(connections) == 0 {
		if hasTombstone {
			return &nodeStateWithTombstone{hasTombstone: true}
		}
		return nil
	}

	// Determine max level from connections if not explicitly set
	maxLevel := level
	for l := range connections {
		if l > maxLevel {
			maxLevel = l
		}
	}

	// Validate that the level fits within packedconn's layer count limit.
	// Normal HNSW levels are ~10-20; anything above MaxLayerCount (64)
	// is corrupt data, likely from a WAL written during an ungraceful shutdown.
	if int(maxLevel)+1 > packedconn.MaxLayerCount {
		if s.logger != nil {
			s.logger.WithFields(logrus.Fields{
				"action":    "hnsw_compactor_skip_corrupt_node",
				"node_id":   nc.NodeID,
				"level":     maxLevel,
				"max_level": packedconn.MaxLayerCount - 1,
			}).Errorf("node level %d exceeds maximum %d, skipping corrupt node",
				maxLevel, packedconn.MaxLayerCount-1)
		}
		return nil
	}

	// Build connections slice
	connSlice := make([][]uint64, maxLevel+1)
	for l := uint16(0); l <= maxLevel; l++ {
		if conns, ok := connections[l]; ok {
			connSlice[l] = conns
		}
	}

	return &nodeStateWithTombstone{
		level:        maxLevel,
		connections:  connSlice,
		hasTombstone: hasTombstone,
	}
}

// nodeStateWithTombstone is a node's absolute state: level, per-level
// connections, and whether it carries a tombstone.
type nodeStateWithTombstone struct {
	level        uint16
	connections  [][]uint64
	hasTombstone bool
}

// Note: Helper functions writeByte, writeBool, writeUint16, writeUint32, writeUint64
// are defined in wal_writer.go and shared across the package.
