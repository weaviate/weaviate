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

package tachiomfresh

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/pkg/errors"
)

// tokenRef is a posting entry: (docID, tokenPos, version) as (u32, u16, u8).
// Version is the document's version in the version map when the entry was
// written: an entry older than its document is stale, as in hfresh. 0
// stands for the first version, 1, so that an entry of a document never
// re-inserted needs no version byte (see newTokenRef).
type tokenRef struct {
	DocID   uint32
	Pos     uint16
	Version uint8
}

// newTokenRef returns the entry of token pos of document docID at version.
// The first version is stored as 0, so that equal entries compare equal.
func newTokenRef(docID uint32, pos uint16, version uint8) tokenRef {
	if version <= 1 {
		version = 0
	}
	return tokenRef{DocID: docID, Pos: pos, Version: version}
}

// version is the entry's version, 1 for an entry written at the first.
func (r tokenRef) version() uint8 {
	return max(r.Version, 1)
}

// key packs the entry's document and position into 8 bytes, docID in the
// high bits. With a posting id, the packed value is the key of the entry's
// reassign task (tokenSet in task_queue.go).
func (r tokenRef) key() uint64 {
	return uint64(r.DocID)<<16 | uint64(r.Pos)
}

// packedPosting is the in-memory form of a posting: its entries packed
// in one byte array, in the image of hfresh's PackedPostingMetadata
// (posting_map.go), a scheme byte followed by fixed-width little-endian
// values. A []tokenRef costs more per entry: 7 bytes of payload and 1 of
// struct padding, plus the slice's spare capacity and headers.
//
// Layout: one width byte (bytes per doc id minus one in the low two bits,
// bytes per position minus one in the third bit, and in the fourth bit
// whether the posting has a version block), then every doc id, then every
// position, then, with the fourth bit, one version byte per entry. The doc
// ids come first and together because stage 1 of a search reads them
// (scanPostings in search.go), so it scans a dense block. A posting has a
// version block only while it holds an entry whose version is above 1: a
// document is re-inserted under its id only after a failed or interrupted
// insert, so most postings never carry one.
//
// The type differs from hfresh's in three ways. An entry here has its own
// fields with their own widths; hfresh has one uint64 per entry, which
// would hold the packed key at 4 bytes, 5 above 65,535 documents. The
// header is one byte; hfresh's is five. A reassign removes one entry;
// hfresh removes no entry from a posting.
//
// The array is exactly sized: an append or a removal allocates a new one
// and copies, about 100 entries. The copy costs far less than the centroid
// search before every append or the token fetch before every removal. The
// array holds no spare capacity, which was a large share of the table. A
// posting is never changed in place, so a reader holding the posting's
// read lock keeps one array for as long as it reads it.
type packedPosting []byte

// versionedFlag marks a posting with a version block.
const versionedFlag = 1 << 3

// widthsFor returns the bytes per doc id and per position for a posting
// holding refs, the narrowest widths that hold its largest values, and
// whether it needs a version block. The widths are per posting, as
// hfresh's scheme is, so that doc ids that keep growing (an update writes
// a new doc id) widen one posting at a time. A posting's doc-id width
// changes when it first gets an id above 255, 65,535 or 16,777,215, three
// times at most in its life. The position width changes when a document
// has more than 256 tokens.
func widthsFor(refs []tokenRef) (docBytes, posBytes int, versioned bool) {
	var maxDoc uint32
	var maxPos uint16
	for _, ref := range refs {
		maxDoc = max(maxDoc, ref.DocID)
		maxPos = max(maxPos, ref.Pos)
		versioned = versioned || ref.Version > 1
	}
	return bytesFor(uint64(maxDoc)), bytesFor(uint64(maxPos)), versioned
}

// pack encodes refs at the widths of widthsFor.
func pack(refs []tokenRef) packedPosting {
	docBytes, posBytes, versioned := widthsFor(refs)
	return encodePosting(refs, docBytes, posBytes, versioned)
}

// encodePosting packs refs at the given widths. Every value must fit, see
// widthsFor; without a version block every entry must be at version 1.
func encodePosting(refs []tokenRef, docBytes, posBytes int, versioned bool) packedPosting {
	if len(refs) == 0 {
		return nil
	}
	entry := docBytes + posBytes
	if versioned {
		entry++
	}
	p := make(packedPosting, 1+len(refs)*entry)
	p[0] = byte(docBytes-1) | byte(posBytes-1)<<2
	if versioned {
		p[0] |= versionedFlag
	}
	docs := p[1:]
	poss := p[1+len(refs)*docBytes:]
	vers := p[1+len(refs)*(docBytes+posBytes):]
	for i, ref := range refs {
		putUint(docs[i*docBytes:], uint64(ref.DocID), docBytes)
		putUint(poss[i*posBytes:], uint64(ref.Pos), posBytes)
		if versioned {
			vers[i] = ref.version()
		}
	}
	return p
}

// putUint writes the n low bytes of v, little-endian, as
// hfresh.PackedPostingMetadata.AddVector does.
func putUint(b []byte, v uint64, n int) {
	for j := range n {
		b[j] = byte(v >> (8 * j))
	}
}

// getUint reads n little-endian bytes.
func getUint(b []byte, n int) uint64 {
	var v uint64
	for j := range n {
		v |= uint64(b[j]) << (8 * j)
	}
	return v
}

func (p packedPosting) widths() (docBytes, posBytes int, versioned bool) {
	return int(p[0]&3) + 1, int(p[0]>>2&1) + 1, p[0]&versionedFlag != 0
}

// len is the number of entries.
func (p packedPosting) len() int {
	if len(p) == 0 {
		return 0
	}
	docBytes, posBytes, versioned := p.widths()
	entry := docBytes + posBytes
	if versioned {
		entry++
	}
	return (len(p) - 1) / entry
}

// blocks returns the doc-id block, the position block and the version
// block, nil without one.
func (p packedPosting) blocks() (docs, poss, vers []byte, docBytes, posBytes int) {
	docBytes, posBytes, versioned := p.widths()
	n := p.len()
	docs = p[1 : 1+n*docBytes]
	poss = p[1+n*docBytes : 1+n*(docBytes+posBytes)]
	if versioned {
		vers = p[1+n*(docBytes+posBytes):]
	}
	return docs, poss, vers, docBytes, posBytes
}

// at returns entry i.
func (p packedPosting) at(i int, docs, poss, vers []byte, docBytes, posBytes int) tokenRef {
	var version uint8
	if vers != nil {
		version = vers[i]
	}
	return newTokenRef(uint32(getUint(docs[i*docBytes:], docBytes)), uint16(getUint(poss[i*posBytes:], posBytes)), version)
}

func (p packedPosting) decode() []tokenRef {
	n := p.len()
	if n == 0 {
		return nil
	}
	docs, poss, vers, docBytes, posBytes := p.blocks()
	refs := make([]tokenRef, n)
	for i := range refs {
		refs[i] = p.at(i, docs, poss, vers, docBytes, posBytes)
	}
	return refs
}

// versions returns the version block, nil when every entry is at version 1.
func (p packedPosting) versions() []byte {
	if len(p) == 0 {
		return nil
	}
	_, _, vers, _, _ := p.blocks()
	return vers
}

// docIDs decodes the doc ids into buf, which is reused when large enough.
// Stage 1 of a search reads them, and the version block when there is one.
// There is one loop per width, so that the compiler turns each decode into
// a plain load.
func (p packedPosting) docIDs(buf []uint32) []uint32 {
	n := p.len()
	if cap(buf) < n {
		buf = make([]uint32, n, 2*n)
	}
	buf = buf[:n]
	if n == 0 {
		return buf
	}
	docs, _, _, docBytes, _ := p.blocks()
	switch docBytes {
	case 1:
		for i := range buf {
			buf[i] = uint32(docs[i])
		}
	case 2:
		for i := range buf {
			buf[i] = uint32(binary.LittleEndian.Uint16(docs[2*i:]))
		}
	case 3:
		for i := range buf {
			buf[i] = uint32(docs[3*i]) | uint32(docs[3*i+1])<<8 | uint32(docs[3*i+2])<<16
		}
	default:
		for i := range buf {
			buf[i] = binary.LittleEndian.Uint32(docs[4*i:])
		}
	}
	return buf
}

// bytesFor is the width that holds v.
func bytesFor(v uint64) int {
	n := 1
	for v >>= 8; v != 0; v >>= 8 {
		n++
	}
	return n
}

// append returns the posting with ref added at the end, in a new array.
// An entry wider than the posting's fields, or one above version 1 in a
// posting without a version block, re-encodes the posting at the widths
// that hold it, as hfresh.PackedPostingMetadata.AddVector upgrades its
// scheme. The fields never narrow here and the block is never dropped; both
// are fitted again when the posting is written whole (see putPosting).
func (p packedPosting) append(ref tokenRef) packedPosting {
	n := p.len()
	if n == 0 {
		return pack([]tokenRef{ref})
	}
	docs, poss, vers, docBytes, posBytes := p.blocks()
	if bytesFor(uint64(ref.DocID)) > docBytes || bytesFor(uint64(ref.Pos)) > posBytes || (ref.Version > 1 && vers == nil) {
		refs := append(p.decode(), ref)
		return encodePosting(refs, max(docBytes, bytesFor(uint64(ref.DocID))), max(posBytes, bytesFor(uint64(ref.Pos))), vers != nil || ref.Version > 1)
	}
	q := make(packedPosting, len(p)+docBytes+posBytes+len(vers)/max(n, 1))
	q[0] = p[0]
	copy(q[1:], docs)
	putUint(q[1+n*docBytes:], uint64(ref.DocID), docBytes)
	copy(q[1+(n+1)*docBytes:], poss)
	putUint(q[1+(n+1)*docBytes+n*posBytes:], uint64(ref.Pos), posBytes)
	if vers != nil {
		base := 1 + (n+1)*(docBytes+posBytes)
		copy(q[base:], vers)
		q[base+n] = ref.version()
	}
	return q
}

// find returns the index of ref in the posting, or -1.
func (p packedPosting) find(ref tokenRef) int {
	n := p.len()
	if n == 0 {
		return -1
	}
	docs, poss, vers, docBytes, posBytes := p.blocks()
	for i := range n {
		if getUint(docs[i*docBytes:], docBytes) != uint64(ref.DocID) ||
			getUint(poss[i*posBytes:], posBytes) != uint64(ref.Pos) {
			continue
		}
		version := uint8(1)
		if vers != nil {
			version = vers[i]
		}
		if version == ref.version() {
			return i
		}
	}
	return -1
}

// contains reports whether ref is in the posting.
func (p packedPosting) contains(ref tokenRef) bool {
	return p.find(ref) >= 0
}

// remove returns the posting without ref, in a new array, and whether ref
// was there. The order within a posting has no meaning, so the last entry
// takes the removed entry's place. The widths and the version block are
// kept.
func (p packedPosting) remove(ref tokenRef) (packedPosting, bool) {
	i := p.find(ref)
	if i < 0 {
		return p, false
	}
	n := p.len()
	if n == 1 {
		return nil, true
	}
	docs, poss, vers, docBytes, posBytes := p.blocks()
	last := n - 1
	verBytes := 0
	if vers != nil {
		verBytes = 1
	}
	q := make(packedPosting, len(p)-docBytes-posBytes-verBytes)
	q[0] = p[0]
	copy(q[1:], docs[:last*docBytes])
	copy(q[1+last*docBytes:], poss[:last*posBytes])
	if vers != nil {
		copy(q[1+last*(docBytes+posBytes):], vers[:last])
	}
	if i != last {
		copy(q[1+i*docBytes:], docs[last*docBytes:n*docBytes])
		copy(q[1+last*docBytes+i*posBytes:], poss[last*posBytes:n*posBytes])
		if vers != nil {
			q[1+last*(docBytes+posBytes)+i] = vers[last]
		}
	}
	return q, true
}

// postingTable holds every posting in memory, indexed by posting id
// (postings carry no vector payload, so they stay in memory). It is
// the read path. The index also writes every change to the posting store
// (appendToken, putPosting). mu guards the growth of lists only. The
// entries of one posting are guarded by the caller's posting lock, as
// hfresh.PostingMap relies on the caller holding it.
type postingTable struct {
	mu    sync.RWMutex
	lists []packedPosting
}

// grow makes lists[id] addressable.
func (p *postingTable) grow(id uint64) {
	p.mu.Lock()
	if uint64(len(p.lists)) <= id {
		p.lists = append(p.lists, make([]packedPosting, id+1-uint64(len(p.lists)))...)
	}
	p.mu.Unlock()
}

// append adds ref to posting id and returns the posting's new length. The
// caller holds postingLocks.Lock(id).
func (p *postingTable) append(id uint64, ref tokenRef) int {
	p.mu.RLock()
	if uint64(len(p.lists)) <= id {
		p.mu.RUnlock()
		p.grow(id)
		p.mu.RLock()
	}
	p.lists[id] = p.lists[id].append(ref)
	n := p.lists[id].len()
	p.mu.RUnlock()
	return n
}

// get returns posting id's entries without copying. The caller holds
// postingLocks.RLock(id) for as long as it reads the array.
func (p *postingTable) get(id uint64) packedPosting {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if uint64(len(p.lists)) <= id {
		return nil
	}
	return p.lists[id]
}

// set replaces posting id's entries. The caller holds
// postingLocks.Lock(id), unless the posting is new and its centroid is not
// searchable yet: no other caller can reach the id then.
func (p *postingTable) set(id uint64, refs []tokenRef) {
	p.grow(id)
	p.mu.RLock()
	p.lists[id] = pack(refs)
	p.mu.RUnlock()
}

// contains reports whether posting id holds ref. The caller holds
// postingLocks.Lock(id) or RLock(id).
func (p *postingTable) contains(id uint64, ref tokenRef) bool {
	return p.get(id).contains(ref)
}

// remove drops ref from posting id and reports whether it was there. The
// caller holds postingLocks.Lock(id).
func (p *postingTable) remove(id uint64, ref tokenRef) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if uint64(len(p.lists)) <= id {
		return false
	}
	next, removed := p.lists[id].remove(ref)
	if removed {
		p.lists[id] = next
	}
	return removed
}

// garbageCollect returns the entries of refs that are live: their document
// is not deleted and they are not older than its version (see live). It has
// the shape of hfresh.Posting.GarbageCollect, but filters into a new slice.
// A split or merge that gives up after collecting therefore still has the
// posting as it was.
func (t *TachiomFresh) garbageCollect(ctx context.Context, refs []tokenRef) ([]tokenRef, error) {
	filtered := make([]tokenRef, 0, len(refs))
	for _, ref := range refs {
		live, err := t.live(ctx, ref)
		if err != nil {
			return nil, err
		}
		if live {
			filtered = append(filtered, ref)
		}
	}
	return filtered, nil
}

// live reports whether entry ref counts: its document is not deleted, and
// the entry is not older than the document's version, hfresh's check
// (hfresh.Posting.GarbageCollect). An older entry was written by an insert
// the document's re-insert replaced.
func (t *TachiomFresh) live(ctx context.Context, ref tokenRef) (bool, error) {
	version, err := t.VersionMap.Get(ctx, uint64(ref.DocID))
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the version of doc %d", ref.DocID)
	}
	return !version.Deleted() && version.Version() <= ref.version(), nil
}

// putPosting replaces posting id on disk and in memory. It is the
// counterpart of hfresh's PostingStore.Put followed by setPostingVectorIDs.
// On disk only the difference with the posting's current entries is
// written (see postingStore.Put in store.go). The caller holds
// postingLocks.Lock(id), or the posting is new and not yet reachable
// through its centroid. The posting's type is registered before the first
// put: the type's entry count follows the posting's length (see
// countEntries in types.go).
func (t *TachiomFresh) putPosting(ctx context.Context, id uint64, refs []tokenRef) error {
	prev := t.postings.get(id).decode()
	if err := t.PostingStore.Put(id, prev, refs); err != nil {
		return errors.Wrapf(err, "failed to put posting %d", id)
	}
	t.countEntries(id, len(refs)-len(prev))
	t.postings.set(id, refs)
	return nil
}

// dropPosting gives up posting id after a split or a merge deleted its
// centroid. hfresh puts an empty posting there "to increase version and
// allow cleanup of old entries on disk"; postingStore.Drop in store.go
// does the same cleanup. The caller holds postingLocks.Lock(id).
func (t *TachiomFresh) dropPosting(id uint64) {
	t.PostingStore.Drop(id)
	t.countEntries(id, -t.postings.get(id).len())
	t.postings.set(id, nil)
}
