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

package segmentindex

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/weaviate/weaviate/entities/lsmkv"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const TREE_KEY_STORE_OVERHEAD = 36

// noChild is what the writers store for an absent child.
const noChild = int64(-1)

// DiskTree is a read-only wrapper around a marshalled index search tree, which
// can be used for reading, but cannot change the underlying structure. It is
// thus perfectly suited as an index for an (immutable) LSM disk segment, but
// pretty much useless for anything else
type DiskTree struct {
	data []byte
	// valueStart and valueEnd bound the region a node's Start and End address,
	// and hasValueBounds says whether a caller supplied them.
	valueStart, valueEnd uint64
	hasValueBounds       bool
}

func NewDiskTree(data []byte) *DiskTree {
	return &DiskTree{
		data: data,
	}
}

// NewDiskTreeWithValueBounds refuses a node addressing bytes outside
// [valueStart, valueEnd), which a tree built without them serves.
func NewDiskTreeWithValueBounds(data []byte, valueStart, valueEnd uint64) *DiskTree {
	return &DiskTree{
		data:           data,
		valueStart:     valueStart,
		valueEnd:       valueEnd,
		hasValueBounds: true,
	}
}

// Get returns the node holding key. Only the matched node's key is materialized
// (callers may keep it beyond the underlying segment's lifetime); the descent
// itself allocates nothing.
func (t *DiskTree) Get(key []byte) (Node, error) {
	return t.materializeNode(t.descend(key, descentEqual))
}

// GetOffsets returns the payload position (start, end) of the node holding key,
// or lsmkv.NotFound. Unlike Get it materializes nothing, so prefer it on hot
// paths that never read Node.Key.
func (t *DiskTree) GetOffsets(key []byte) (start, end uint64, err error) {
	return t.offsetsAt(t.descend(key, descentEqual))
}

// Contains reports whether the tree holds key, without materializing it. It
// calls GetOffsets, so any node Get refuses to serve is not present here
// either.
func (t *DiskTree) Contains(key []byte) (bool, error) {
	if _, _, err := t.GetOffsets(key); err != nil {
		if errors.Is(err, lsmkv.NotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Seek returns the node holding the smallest key >= key, or lsmkv.NotFound if
// the tree holds no such key. Only the matched node's key is materialized; the
// descent itself allocates nothing.
func (t *DiskTree) Seek(key []byte) (Node, error) {
	return t.materializeNode(t.descend(key, descentGreaterThanEqual))
}

// SeekOffsets returns the payload position (start, end) of the node holding the
// smallest key >= key, or lsmkv.NotFound. Unlike Seek it allocates nothing, so
// prefer it wherever the key is not read — a segment cursor takes its key from
// the payload it goes on to parse.
func (t *DiskTree) SeekOffsets(key []byte) (start, end uint64, err error) {
	return t.offsetsAt(t.descend(key, descentGreaterThanEqual))
}

// Next returns the node holding the smallest key strictly greater than key, or
// lsmkv.NotFound if the tree holds no such key.
func (t *DiskTree) Next(key []byte) (Node, error) {
	return t.materializeNode(t.descend(key, descentGreaterThan))
}

// offsetsAt reads the payload bounds of the node a descent ended on. The
// descent's bounds checks are what make the loads here safe.
func (t *DiskTree) offsetsAt(d descentResult, err error) (uint64, uint64, error) {
	if err != nil {
		return 0, 0, err
	}
	pos := d.pos
	// errors name the node's own start, where an operator hexdumping has to look
	nodeStart := pos - d.keyLen - 4
	start := binary.LittleEndian.Uint64(t.data[pos:])
	end := binary.LittleEndian.Uint64(t.data[pos+8:])
	if end < start {
		// segment.get sizes its read with end-start, so this would allocate near 2^64
		return 0, 0, corruptIndexf("node at %d: value ends at %d, before its start %d", nodeStart, end, start)
	}
	if err := t.checkValueRange(nodeStart, start, end); err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// checkValueRange rejects a node addressing bytes outside the region the index
// was built for. Direction alone is not enough: a forward range can still run
// past the end.
func (t *DiskTree) checkValueRange(nodeStart, start, end uint64) error {
	if !t.hasValueBounds {
		return nil
	}
	if start < t.valueStart || end > t.valueEnd {
		return corruptIndexf("node at %d: value [%d,%d) outside the addressable range [%d,%d)",
			nodeStart, start, end, t.valueStart, t.valueEnd)
	}
	return nil
}

// corruptIndexf builds every error this file reports for data it cannot read.
// segment.reportIndexErr and makeKeyExistsOnUpperSegments match on the sentinel
// with errors.Is, so a site reporting without it opts out of both silently.
func corruptIndexf(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{lsmkv.ErrCorruptIndex}, a...)...)
}

// materializeNode builds the node a descent ended on, cloning its key because
// the caller can outlive the mmapped segment. The descent's bounds checks are
// what make the loads here safe.
func (t *DiskTree) materializeNode(d descentResult, err error) (Node, error) {
	start, end, err := t.offsetsAt(d, err)
	if err != nil {
		return Node{}, err
	}
	return Node{
		Key:   bytes.Clone(t.data[d.pos-d.keyLen : d.pos]),
		Start: start,
		End:   end,
	}, nil
}

// descentResult is the offset just past a matched node's key, where its
// start/end fields begin, plus that key's length. Kept as a struct rather than
// two uint64 returns so offsetsAt and materializeNode can't mix up which is which.
type descentResult struct {
	pos    uint64
	keyLen uint64
}

// descentMode is the relation a descent's answer holds to the probe key.
type descentMode uint8

const (
	// descentEqual fails unless the probe key itself is present.
	descentEqual descentMode = iota
	// descentGreaterThanEqual returns the smallest key >= the probe.
	descentGreaterThanEqual
	// descentGreaterThan returns the smallest key > the probe.
	descentGreaterThan
)

// descend jumps through the buffer to the node mode selects. Keys are compared
// in place, so the descent allocates nothing.
//
// A seek answers with the last node it turned left at, unless the mode takes an
// exact match and finds one: turning right rules out that node and everything
// left of it, so no key above the probe is missed.
func (t *DiskTree) descend(key []byte, mode descentMode) (descentResult, error) {
	if len(t.data) == 0 {
		return descentResult{}, lsmkv.NotFound
	}
	var pos, keyLen uint64
	data := t.data
	dataLen := uint64(len(data))
	steps := 0
	maxSteps := maxDescentSteps(len(data))

	// 8-byte keys (the int/number/date/docID encodings) compare as big-endian
	// uint64s: for equal-length keys, lexicographic byte order equals numeric
	// big-endian order, and the single-word compare avoids the bytes.Compare
	// call that otherwise dominates the descent. The probe key's word is
	// loop-invariant, read once here.
	var probeWord uint64
	probe8 := len(key) == 8
	if probe8 {
		probeWord = binary.BigEndian.Uint64(key)
	}

	// deliberately without a default arm: the exhaustive linter treats one as
	// covering every case, so a mode added later would compile into silence
	var acceptEqual, keepCandidate bool
	switch mode {
	case descentEqual:
		acceptEqual = true
	case descentGreaterThanEqual:
		acceptEqual, keepCandidate = true, true
	case descentGreaterThan:
		keepCandidate = true
	}
	// the last node the descent turned left at, as an offset past its key
	var candidatePos, candidateKeyLen uint64
	haveCandidate := false

	for {
		// A child pointer leading back to an already visited node would keep the
		// descent going forever. No descent visits a node twice, so it cannot take
		// more steps than the buffer can hold nodes.
		steps++
		if steps > maxSteps {
			return descentResult{}, corruptIndexf("cyclic child pointers, past %d nodes at offset %d (buffer %d)",
				maxSteps, pos, dataLen)
		}

		// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8].
		// pos is 0 or a non-negative child pointer, so pos+4 cannot wrap.
		nodePos := pos
		if pos+4 > dataLen {
			// no node fits here: a child pointer past the buffer, or a buffer too
			// short for the root. Either way corruption, not an absent key.
			return descentResult{}, corruptIndexf("node at %d out of range (buffer %d)", pos, dataLen)
		}

		keyLen = uint64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if keyLen > dataLen-pos {
			return descentResult{}, corruptIndexf("node key at %d len %d out of range (%d bytes available)", pos, keyLen, dataLen-pos)
		}

		var keyEqual int
		if probe8 && keyLen == 8 {
			// the keyLen bounds check above guarantees 8 readable bytes
			keyEqual = cmp.Compare(probeWord, binary.BigEndian.Uint64(data[pos:]))
		} else {
			keyEqual = bytes.Compare(key, data[pos:pos+keyLen])
		}
		pos += keyLen
		// every node the writer emits carries the full 32-byte trailer. Judging it
		// per arm needs only 16 to answer an exact match, which served a truncated
		// node that Next, continuing past the same match, refused
		if avail := dataLen - pos; avail < 32 {
			return descentResult{}, corruptIndexf("node value at %d out of range (%d bytes available, need 32)", pos, avail)
		}

		if keyEqual == 0 && acceptEqual {
			return descentResult{pos: pos, keyLen: keyLen}, nil
		}

		var child int64
		if keyEqual < 0 {
			if keepCandidate {
				candidatePos, candidateKeyLen, haveCandidate = pos, keyLen, true
			}
			child = int64(binary.LittleEndian.Uint64(data[pos+16:])) // skip start+end
		} else {
			child = int64(binary.LittleEndian.Uint64(data[pos+24:])) // skip start+end+left
		}

		if child == noChild { // the descent ends here
			if !haveCandidate {
				return descentResult{}, lsmkv.NotFound
			}
			return descentResult{pos: candidatePos, keyLen: candidateKeyLen}, nil
		}
		if child < 0 {
			// any other negative value is corruption, and -2, -3 and -4 would wrap
			// pos+4 back under dataLen, past the bounds check and into a panic
			return descentResult{}, corruptIndexf("node at %d has child pointer %d", nodePos, child)
		}
		pos = uint64(child)
	}
}

// maxDescentSteps bounds a root-to-leaf descent at the number of nodes a buffer
// of this size can hold, which no descent following intact child pointers
// reaches. It is what makes a cyclic pointer terminate.
func maxDescentSteps(dataLen int) int {
	return dataLen/TREE_KEY_STORE_OVERHEAD + 1
}

// readNodeKey returns the key of the node at the start of in, and the node's total
// size so a sequential walk can find the next one. It reports io.EOF when in is
// too short to hold a node at all.
func (t *DiskTree) readNodeKey(in []byte) ([]byte, int, error) {
	// in buffer needs at least 36 bytes of data:
	// 4bytes for key length, 32bytes for position and children
	if len(in) < TREE_KEY_STORE_OVERHEAD {
		return nil, 0, io.EOF
	}

	rw := byteops.NewReadWriter(in)

	keyLen := uint64(rw.ReadUint32())
	// the whole node is keyLen + TREE_KEY_STORE_OVERHEAD bytes; the len check
	// above only covers a zero-length key. Reject a keyLen that would push the
	// key or the fixed trailer past the buffer (corrupt/truncated index) so the
	// reads below cannot panic. Compared against len(in)-overhead to avoid wrap.
	if keyLen > uint64(len(in))-TREE_KEY_STORE_OVERHEAD {
		return nil, int(rw.Position), corruptIndexf("node key len %d out of range (%d bytes for a key here)",
			keyLen, uint64(len(in))-TREE_KEY_STORE_OVERHEAD)
	}
	key, err := rw.CopyBytesFromBuffer(keyLen, nil)
	if err != nil {
		return nil, int(rw.Position), fmt.Errorf("copy node key: %w", err)
	}

	return key, int(rw.Position) + 32, nil // start + end + both children
}

// AllKeys is a relatively expensive operation as it basically does a full disk
// read of the index. It is meant for one of operations, such as initializing a
// segment where we need access to all keys, e.g. to build a bloom filter. This
// should not run at query time.
//
// Keys are returned in the tree's on-disk (serialized) order, which is not
// sorted. Do not use this method if an In-Order traversal is required, but only
// for use cases who don't require a specific order, such as building a
// bloom filter.
func (t *DiskTree) AllKeys() ([][]byte, error) {
	var out [][]byte
	bufferPos := 0
	for {
		key, readLength, err := t.readNodeKey(t.data[bufferPos:])
		bufferPos += readLength
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		out = append(out, key)
	}

	return out, nil
}

func (t *DiskTree) Size() int {
	return len(t.data)
}

// KeyCount returns the number of keys in the tree without allocating.
// It walks through the serialized nodes, skipping over each one.
func (t *DiskTree) KeyCount() int {
	count := 0
	bufferPos := 0
	// each node: 4 (keyLen) + keyLen + 8 (start) + 8 (end) + 8 (left) + 8 (right)
	for bufferPos+TREE_KEY_STORE_OVERHEAD <= len(t.data) {
		keyLen := int(binary.LittleEndian.Uint32(t.data[bufferPos:]))
		nodeSize := keyLen + TREE_KEY_STORE_OVERHEAD
		if bufferPos+nodeSize > len(t.data) {
			break
		}
		bufferPos += nodeSize
		count++
	}
	return count
}

// ForEachKey iterates over all keys in the tree without allocating a slice.
// The key passed to fn is a subslice of the underlying data and must not
// be retained or modified by the caller.
func (t *DiskTree) ForEachKey(fn func(key []byte)) {
	bufferPos := 0
	for bufferPos+TREE_KEY_STORE_OVERHEAD <= len(t.data) {
		keyLen := int(binary.LittleEndian.Uint32(t.data[bufferPos:]))
		nodeSize := keyLen + TREE_KEY_STORE_OVERHEAD
		if bufferPos+nodeSize > len(t.data) {
			break
		}
		fn(t.data[bufferPos+4 : bufferPos+4+keyLen])
		bufferPos += nodeSize
	}
}

// ForEachNodeInRange walks the serialized nodes packed in data[from:to) — the
// tree's on-disk order, not key order — without allocating. The key passed to fn
// is a subslice of the underlying data, valid only for the duration of fn.
// Bounds must be node-aligned, e.g. from SplitNodeRanges.
//
// A tail too short to hold a node ends the walk, as it does in AllKeys and
// KeyCount: a segment written with checksums carries a 4-byte trailer here when
// no secondary index bounds the primary, so a short tail is not a corruption
// signal. A node whose header does not parse still is, and errors.
func (t *DiskTree) ForEachNodeInRange(from, to int, fn func(key []byte, start, end uint64) error) error {
	if from < 0 || to > len(t.data) || from > to {
		return corruptIndexf("node range [%d,%d) outside index bounds [0,%d]", from, to, len(t.data))
	}
	pos := from
	for pos < to {
		remaining := to - pos
		if remaining < TREE_KEY_STORE_OVERHEAD {
			return nil
		}
		keyLen := int(binary.LittleEndian.Uint32(t.data[pos:]))
		if keyLen > remaining-TREE_KEY_STORE_OVERHEAD {
			return corruptIndexf("node at %d: key len %d exceeds remaining %d bytes",
				pos, uint32(keyLen), remaining-TREE_KEY_STORE_OVERHEAD)
		}
		keyEnd := pos + 4 + keyLen
		if err := fn(t.data[pos+4:keyEnd],
			binary.LittleEndian.Uint64(t.data[keyEnd:]),
			binary.LittleEndian.Uint64(t.data[keyEnd+8:])); err != nil {
			return err
		}
		pos += keyLen + TREE_KEY_STORE_OVERHEAD
	}
	return nil
}

// SplitNodeRanges returns node-aligned [from,to) byte ranges that partition the
// serialized index into at most parts pieces of roughly equal byte size, for use
// with ForEachNodeInRange. An empty tree yields nil. A node that does not parse
// stops further splitting and leaves the rest in the last range, so the walker
// reports the corruption rather than this silently trimming the scan.
func (t *DiskTree) SplitNodeRanges(parts int) [][2]int {
	n := len(t.data)
	if n == 0 {
		return nil
	}
	// a range holds at least one node, so parts beyond that ceiling can only add
	// boundary arithmetic. Clamping keeps the walk O(nodes) for any caller value.
	if ceiling := n/TREE_KEY_STORE_OVERHEAD + 1; parts > ceiling {
		parts = ceiling
	}
	if parts <= 1 {
		return [][2]int{{0, n}}
	}
	ranges := make([][2]int, 0, parts)
	start, pos, next := 0, 0, 1
	for next < parts && pos < n {
		if n-pos < TREE_KEY_STORE_OVERHEAD {
			break
		}
		keyLen := int(binary.LittleEndian.Uint32(t.data[pos:]))
		if keyLen > n-pos-TREE_KEY_STORE_OVERHEAD {
			break
		}
		pos += keyLen + TREE_KEY_STORE_OVERHEAD
		if pos >= n*next/parts {
			if pos < n {
				ranges = append(ranges, [2]int{start, pos})
				start = pos
			}
			for next < parts && pos >= n*next/parts {
				next++
			}
		}
	}
	return append(ranges, [2]int{start, n})
}
