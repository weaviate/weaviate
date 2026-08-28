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
}

func NewDiskTree(data []byte) *DiskTree {
	return &DiskTree{
		data: data,
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
// answers off GetOffsets so that a node Get refuses to serve — one whose
// payload range runs backwards — is not reported as present here.
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

// offsetsAt reads the payload bounds of the node a descent ended on. pos must
// come from a descent, whose bounds checks are what make the loads here safe.
func (t *DiskTree) offsetsAt(pos, _ uint64, err error) (uint64, uint64, error) {
	if err != nil {
		return 0, 0, err
	}
	start := binary.LittleEndian.Uint64(t.data[pos:])
	end := binary.LittleEndian.Uint64(t.data[pos+8:])
	if end < start {
		return 0, 0, reversedPayloadRange(start, end)
	}
	return start, end, nil
}

// reversedPayloadRange reports a payload range that runs backwards, which a
// caller sizing a read with end-start would turn into a near-2^64 allocation.
func reversedPayloadRange(start, end uint64) error {
	return fmt.Errorf("node payload ends at %d, before its start %d", end, start)
}

// materializeNode builds the node a descent ended on, cloning its key because
// the caller can outlive the mmapped segment. pos and keyLen must come from a
// descent, whose bounds checks are what make the loads here safe.
func (t *DiskTree) materializeNode(pos, keyLen uint64, err error) (Node, error) {
	if err != nil {
		return Node{}, err
	}
	start := binary.LittleEndian.Uint64(t.data[pos:])
	end := binary.LittleEndian.Uint64(t.data[pos+8:])
	if end < start {
		return Node{}, reversedPayloadRange(start, end)
	}
	return Node{
		Key:   bytes.Clone(t.data[pos-keyLen : pos]),
		Start: start,
		End:   end,
	}, nil
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

// descend jumps through the buffer to the node mode selects, returning the
// offset just past its key (where the start/end fields begin) and the key
// length. Keys are compared in place, so the descent allocates nothing.
//
// A seek answers with the last node it turned left at, unless the mode takes an
// exact match and finds one: turning right rules out that node and everything
// left of it, so no key above the probe is missed. An absent key yields
// NotFound and corrupt data an error, never a panic — a caller that reads
// corruption as absence drops keys that are there.
func (t *DiskTree) descend(key []byte, mode descentMode) (pos, keyLen uint64, err error) {
	if len(t.data) == 0 {
		return 0, 0, lsmkv.NotFound
	}
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

	// what the mode asks of the loop: whether an exact match is an answer, and
	// whether a key above the probe can stand in for one. Resolved once here, and
	// deliberately without a default arm — the exhaustive linter treats one as
	// covering every case, so a mode added later would compile into silence.
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
			return 0, 0, fmt.Errorf("cyclic child pointers in segment index: past %d nodes at offset %d (buffer %d)",
				maxSteps, pos, dataLen)
		}

		// node layout: [keyLen:4][key:keyLen][start:8][end:8][left:8][right:8].
		// pos is 0 or a non-negative child pointer, so pos+4 cannot wrap.
		nodePos := pos
		if pos+4 > dataLen {
			// no node fits here: a child pointer past the buffer, or a buffer too
			// short for the root. Either way corruption, not an absent key.
			return 0, 0, fmt.Errorf("node at %d out of range (buffer %d)", pos, dataLen)
		}

		keyLen = uint64(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if keyLen > dataLen-pos {
			return 0, 0, fmt.Errorf("node key at %d len %d out of range (%d bytes available)", pos, keyLen, dataLen-pos)
		}

		var keyEqual int
		if probe8 && keyLen == 8 {
			// the keyLen bounds check above guarantees 8 readable bytes
			keyEqual = cmp.Compare(probeWord, binary.BigEndian.Uint64(data[pos:]))
		} else {
			keyEqual = bytes.Compare(key, data[pos:pos+keyLen])
		}
		pos += keyLen
		avail := dataLen - pos

		if keyEqual == 0 && acceptEqual {
			if avail < 16 { // start + end
				return 0, 0, fmt.Errorf("node value at %d out of range (%d bytes available, need 16)", pos, avail)
			}
			return pos, keyLen, nil
		}

		var child int64
		if keyEqual < 0 {
			if avail < 24 { // start + end + left child
				return 0, 0, fmt.Errorf("node value at %d out of range (%d bytes available, need 24 to reach the left child)", pos, avail)
			}
			if keepCandidate {
				candidatePos, candidateKeyLen, haveCandidate = pos, keyLen, true
			}
			child = int64(binary.LittleEndian.Uint64(data[pos+16:])) // skip start+end
		} else {
			if avail < 32 { // start + end + left + right child
				return 0, 0, fmt.Errorf("node value at %d out of range (%d bytes available, need 32 to reach the right child)", pos, avail)
			}
			child = int64(binary.LittleEndian.Uint64(data[pos+24:])) // skip start+end+left
		}

		if child == noChild { // the descent ends here
			if !haveCandidate {
				return 0, 0, lsmkv.NotFound
			}
			return candidatePos, candidateKeyLen, nil
		}
		if child < 0 {
			// any other negative value is corruption, and -2, -3 and -4 would wrap
			// pos+4 back under dataLen, past the bounds check and into a panic
			return 0, 0, fmt.Errorf("node at %d has child pointer %d", nodePos, child)
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
		return nil, int(rw.Position), fmt.Errorf("node key len %d out of range (%d bytes for a key here)",
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
		return fmt.Errorf("node range [%d,%d) outside index bounds [0,%d]", from, to, len(t.data))
	}
	pos := from
	for pos < to {
		remaining := to - pos
		if remaining < TREE_KEY_STORE_OVERHEAD {
			return nil
		}
		keyLen := int(binary.LittleEndian.Uint32(t.data[pos:]))
		if keyLen > remaining-TREE_KEY_STORE_OVERHEAD {
			return fmt.Errorf("node at %d: key len %d exceeds remaining %d bytes",
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
