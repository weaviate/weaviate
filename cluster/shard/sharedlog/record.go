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

package sharedlog

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// On-disk framing. Every segment starts with an 8-byte header (magic,
// version, padding); records follow back to back:
//
//	[bodyLen u32][crc32c u32 over body][body]
//	body: [type u8][groupID u64] then per type:
//	  recTypeWrite / recTypeWriteCopy:
//	    [flags u8][entryCount u32]
//	    per entry: [index u64][term u64][entLen u32][marshaled raftpb.Entry]
//	    if flagHardState: [len u32][marshaled raftpb.HardState]
//	    if flagConfState: [len u32][marshaled raftpb.ConfState]
//	    if flagSnapshot:  [len u32][marshaled raftpb.Snapshot]
//	  recTypeTombstone: (nothing)
//
// All integers little-endian. Entry index/term ride in the frame so replay
// and Term() never unmarshal entry payloads. One recTypeWrite record carries
// one whole GroupWrite — the crash-atomicity unit (see the package doc in
// wal.go).
const (
	segMagic      uint32 = 0x57534C31 // "WSL1"
	segVersion    byte   = 1
	segHeaderSize        = 8
	recHeaderSize        = 8

	recTypeWrite     byte = 1
	recTypeWriteCopy byte = 2
	recTypeTombstone byte = 3

	flagHardState byte = 1 << 0
	flagConfState byte = 1 << 1
	flagSnapshot  byte = 1 << 2

	// entryFrameSize is the per-entry frame overhead inside a write record:
	// index u64 + term u64 + payload length u32.
	entryFrameSize = 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// entryMeta locates one entry's marshaled payload (off/n) plus the raft
// index and term carried in its frame.
type entryMeta struct {
	index uint64
	term  uint64
	off   int64
	n     uint32
}

// decodedWrite is one write/copy record in index-applicable form: the
// unmarshaled singleton states plus payload spans. Both the runtime encoder
// and replay decode produce it, so the write path and restart rebuild fold
// records into the index through the same apply functions.
//
// Offsets are relative to the encode buffer until rebase points them into
// the segment they were written to; decodeRecord emits them absolute.
type decodedWrite struct {
	groupID uint64
	copyRec bool

	entries []entryMeta

	hasHS bool
	hs    raftpb.HardState
	hsN   uint32

	hasCS bool
	cs    raftpb.ConfState
	csN   uint32

	hasSnap  bool
	snapMeta raftpb.SnapshotMetadata
	snapOff  int64
	snapN    uint32

	seg *segment
}

func (d *decodedWrite) rebase(seg *segment, delta int64) {
	d.seg = seg
	for i := range d.entries {
		d.entries[i].off += delta
	}
	if d.hasSnap {
		d.snapOff += delta
	}
}

// rawEntry is one entry's frame material for appendWriteRecord.
type rawEntry struct {
	index uint64
	term  uint64
	data  []byte
}

// appendWriteRecord frames one write/copy record into buf and returns the
// extended buffer plus the record's decodedWrite with buf-relative spans.
// The raw blobs and their unmarshaled counterparts must describe the same
// values; nil raw blobs mean "absent".
func appendWriteRecord(buf []byte, typ byte, groupID uint64, entries []rawEntry,
	hs *raftpb.HardState, cs *raftpb.ConfState, snapMeta *raftpb.SnapshotMetadata,
	hsRaw, csRaw, snapRaw []byte,
) ([]byte, decodedWrite) {
	recStart := len(buf)
	buf = append(buf, make([]byte, recHeaderSize)...)
	bodyStart := len(buf)

	buf = append(buf, typ)
	buf = binary.LittleEndian.AppendUint64(buf, groupID)

	var flags byte
	if hsRaw != nil {
		flags |= flagHardState
	}
	if csRaw != nil {
		flags |= flagConfState
	}
	if snapRaw != nil {
		flags |= flagSnapshot
	}
	buf = append(buf, flags)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entries)))

	d := decodedWrite{groupID: groupID, copyRec: typ == recTypeWriteCopy}
	if len(entries) > 0 {
		d.entries = make([]entryMeta, 0, len(entries))
	}
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint64(buf, e.index)
		buf = binary.LittleEndian.AppendUint64(buf, e.term)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.data)))
		d.entries = append(d.entries, entryMeta{
			index: e.index, term: e.term, off: int64(len(buf)), n: uint32(len(e.data)),
		})
		buf = append(buf, e.data...)
	}
	if hsRaw != nil {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(hsRaw)))
		buf = append(buf, hsRaw...)
		d.hasHS, d.hs, d.hsN = true, *hs, uint32(len(hsRaw))
	}
	if csRaw != nil {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(csRaw)))
		buf = append(buf, csRaw...)
		d.hasCS, d.cs, d.csN = true, *cs, uint32(len(csRaw))
	}
	if snapRaw != nil {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(snapRaw)))
		d.snapOff = int64(len(buf))
		buf = append(buf, snapRaw...)
		d.hasSnap, d.snapMeta, d.snapN = true, *snapMeta, uint32(len(snapRaw))
	}

	body := buf[bodyStart:]
	binary.LittleEndian.PutUint32(buf[recStart:], uint32(len(body)))
	binary.LittleEndian.PutUint32(buf[recStart+4:], crc32.Checksum(body, castagnoli))
	return buf, d
}

func appendTombstoneRecord(buf []byte, groupID uint64) []byte {
	recStart := len(buf)
	buf = append(buf, make([]byte, recHeaderSize)...)
	bodyStart := len(buf)
	buf = append(buf, recTypeTombstone)
	buf = binary.LittleEndian.AppendUint64(buf, groupID)
	body := buf[bodyStart:]
	binary.LittleEndian.PutUint32(buf[recStart:], uint32(len(body)))
	binary.LittleEndian.PutUint32(buf[recStart+4:], crc32.Checksum(body, castagnoli))
	return buf
}

// encodeGroupWrite marshals gw and frames it as one recTypeWrite record.
// Returns emitted=false (and appends nothing) for a GroupWrite with no
// persistable payload. An empty snapshot is skipped, mirroring the previous
// engine's applyWrite.
func encodeGroupWrite(buf []byte, gw *GroupWrite) (_ []byte, d decodedWrite, emitted bool, err error) {
	var entries []rawEntry
	if len(gw.Entries) > 0 {
		entries = make([]rawEntry, len(gw.Entries))
		for i := range gw.Entries {
			data, err := gw.Entries[i].Marshal()
			if err != nil {
				return buf, decodedWrite{}, false, fmt.Errorf("marshal entry: %w", err)
			}
			entries[i] = rawEntry{index: gw.Entries[i].Index, term: gw.Entries[i].Term, data: data}
		}
	}
	var hsRaw, csRaw, snapRaw []byte
	if gw.HardState != nil {
		if hsRaw, err = gw.HardState.Marshal(); err != nil {
			return buf, decodedWrite{}, false, fmt.Errorf("marshal hardstate: %w", err)
		}
	}
	if gw.ConfState != nil {
		if csRaw, err = gw.ConfState.Marshal(); err != nil {
			return buf, decodedWrite{}, false, fmt.Errorf("marshal confstate: %w", err)
		}
	}
	var snapMeta *raftpb.SnapshotMetadata
	if gw.Snapshot != nil && !raft.IsEmptySnap(*gw.Snapshot) {
		if snapRaw, err = gw.Snapshot.Marshal(); err != nil {
			return buf, decodedWrite{}, false, fmt.Errorf("marshal snapshot: %w", err)
		}
		snapMeta = &gw.Snapshot.Metadata
	}
	if len(entries) == 0 && hsRaw == nil && csRaw == nil && snapRaw == nil {
		return buf, decodedWrite{}, false, nil
	}
	buf, d = appendWriteRecord(buf, recTypeWrite, gw.GroupID, entries,
		gw.HardState, gw.ConfState, snapMeta, hsRaw, csRaw, snapRaw)
	return buf, d, true, nil
}

// decodeRecord parses one CRC-validated record body. bodyOff is the body's
// absolute offset within seg's file; spans in the result are absolute. A
// decode error on a CRC-valid body is a format violation, not a torn write —
// callers must fail loudly, never truncate.
func decodeRecord(body []byte, seg *segment, bodyOff int64) (typ byte, d decodedWrite, err error) {
	p := 0
	u8 := func() (byte, error) {
		if p+1 > len(body) {
			return 0, fmt.Errorf("record body truncated at byte %d", p)
		}
		v := body[p]
		p++
		return v, nil
	}
	u32 := func() (uint32, error) {
		if p+4 > len(body) {
			return 0, fmt.Errorf("record body truncated at byte %d", p)
		}
		v := binary.LittleEndian.Uint32(body[p:])
		p += 4
		return v, nil
	}
	u64 := func() (uint64, error) {
		if p+8 > len(body) {
			return 0, fmt.Errorf("record body truncated at byte %d", p)
		}
		v := binary.LittleEndian.Uint64(body[p:])
		p += 8
		return v, nil
	}
	blob := func() ([]byte, int64, error) {
		n, err := u32()
		if err != nil {
			return nil, 0, err
		}
		if p+int(n) > len(body) {
			return nil, 0, fmt.Errorf("record blob of %d bytes overruns body at byte %d", n, p)
		}
		b := body[p : p+int(n)]
		off := bodyOff + int64(p)
		p += int(n)
		return b, off, nil
	}

	if typ, err = u8(); err != nil {
		return 0, decodedWrite{}, err
	}
	if d.groupID, err = u64(); err != nil {
		return 0, decodedWrite{}, err
	}

	switch typ {
	case recTypeTombstone:
		if p != len(body) {
			return 0, decodedWrite{}, fmt.Errorf("tombstone record carries %d trailing bytes", len(body)-p)
		}
		return typ, d, nil
	case recTypeWrite, recTypeWriteCopy:
		d.copyRec = typ == recTypeWriteCopy
		d.seg = seg
	default:
		return 0, decodedWrite{}, fmt.Errorf("unknown record type %d", typ)
	}

	flags, err := u8()
	if err != nil {
		return 0, decodedWrite{}, err
	}
	if flags&^(flagHardState|flagConfState|flagSnapshot) != 0 {
		return 0, decodedWrite{}, fmt.Errorf("unknown record flags %#x", flags)
	}
	count, err := u32()
	if err != nil {
		return 0, decodedWrite{}, err
	}
	if count > 0 {
		d.entries = make([]entryMeta, 0, count)
	}
	for i := uint32(0); i < count; i++ {
		var e entryMeta
		if e.index, err = u64(); err != nil {
			return 0, decodedWrite{}, err
		}
		if e.term, err = u64(); err != nil {
			return 0, decodedWrite{}, err
		}
		if e.n, err = u32(); err != nil {
			return 0, decodedWrite{}, err
		}
		if p+int(e.n) > len(body) {
			return 0, decodedWrite{}, fmt.Errorf("entry payload of %d bytes overruns body at byte %d", e.n, p)
		}
		e.off = bodyOff + int64(p)
		p += int(e.n)
		d.entries = append(d.entries, e)
	}
	if flags&flagHardState != 0 {
		raw, _, err := blob()
		if err != nil {
			return 0, decodedWrite{}, err
		}
		if err := d.hs.Unmarshal(raw); err != nil {
			return 0, decodedWrite{}, fmt.Errorf("unmarshal hardstate: %w", err)
		}
		d.hasHS, d.hsN = true, uint32(len(raw))
	}
	if flags&flagConfState != 0 {
		raw, _, err := blob()
		if err != nil {
			return 0, decodedWrite{}, err
		}
		if err := d.cs.Unmarshal(raw); err != nil {
			return 0, decodedWrite{}, fmt.Errorf("unmarshal confstate: %w", err)
		}
		d.hasCS, d.csN = true, uint32(len(raw))
	}
	if flags&flagSnapshot != 0 {
		raw, off, err := blob()
		if err != nil {
			return 0, decodedWrite{}, err
		}
		var snap raftpb.Snapshot
		if err := snap.Unmarshal(raw); err != nil {
			return 0, decodedWrite{}, fmt.Errorf("unmarshal snapshot: %w", err)
		}
		d.hasSnap, d.snapMeta, d.snapOff, d.snapN = true, snap.Metadata, off, uint32(len(raw))
	}
	if p != len(body) {
		return 0, decodedWrite{}, fmt.Errorf("record carries %d trailing bytes", len(body)-p)
	}
	return typ, d, nil
}
