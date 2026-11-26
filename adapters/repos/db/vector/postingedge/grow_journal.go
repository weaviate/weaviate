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

package postingedge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// The journal of the index that grows.
//
// A write of the index is many writes: an insert puts a member in a row of
// the bucket of postings, an entry in the posting map, a routing code, the
// distances of its edges, the changes to the lists of its neighbors, its
// vector in a file and its slot in another, and when its posting splits two
// rows more, a map entry for every member and two centroids. Every bucket
// has a log of its own, which reaches the system when its buffer fills, and
// the files are written as they come. A process that dies in the middle
// leaves some of those writes on disk and not the others, and what is on
// disk is then not an index: the map counts a vector the meta does not, a
// member has no list of edges, a posting was split and one of its halves is
// not there.
//
// So a write of the index, a step, goes to the journal first, whole. While
// the step runs its writes wait in memory, where the step itself reads them
// back; when it ends they are appended to the journal as one record, with
// the meta the index has after them, in one write to the system; and only
// then do they go to the buckets and the files. An index that opens applies
// again the records the journal holds, which is harmless where they were
// applied already, and leaves out a record that is not whole: that step
// never happened. What the index is after a crash is what it was after the
// last step whose record was written.
//
// The journal is emptied when everything it tells has been handed to the
// system by its own way (sync): the logs of the buckets written, and the
// meta. It protects against the death of the process, which is what loses
// the buffers of the logs. Against the death of the machine it gives what
// the logs of the store give, which are not synced either.
//
// The graph over the centroids is not in the journal: it has a commit log
// of its own, which a crash can leave behind or ahead of the index. A file
// marks that the graph has changed since the journal was last emptied, and
// an index that opens with the mark builds the graph again from its
// centroids (rebuildCentroidGraph).

const (
	growJournalFile = "journal.log"
	// growGraphMark is the file that tells that the graph over the centroids
	// changed since the journal was last emptied.
	growGraphMark = "graph.changed"
	// growJournalLimit is the size past which the index empties its journal
	// by itself, without waiting for whoever syncs it.
	growJournalLimit = 64 << 20
)

// growTarget is where a write of a step goes: a bucket or a file.
type growTarget byte

const (
	growTargetPostings growTarget = iota + 1
	growTargetMap
	growTargetRouting
	growTargetDists
	growTargetDeleted
	growTargetEdges
	growTargetPending
	growTargetVectors
	growTargetSlots
	growTargetBuilt
	growTargetCentroids
	growTargets
)

type growEntryKind byte

const (
	growEntryPut growEntryKind = iota + 1
	growEntryDelete
	growEntrySetAdd
	growEntrySetDelete
	growEntryFile
)

// growEntry is one write of a step.
type growEntry struct {
	kind   growEntryKind
	target growTarget
	key    []byte
	// values are what is written: one value, but for the values added to a
	// row at once.
	values [][]byte
	// offset is where in its file a write of a file goes.
	offset int64
}

// growRecord is a step as the journal holds it.
type growRecord struct {
	meta    []byte
	entries []growEntry
}

func appendUint32(buf []byte, x int) []byte {
	return binary.LittleEndian.AppendUint32(buf, uint32(x))
}

// encode appends the record to buf: its length, its checksum and its
// payload.
func (r *growRecord) encode(buf []byte) []byte {
	start := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	buf = appendUint32(buf, len(r.meta))
	buf = append(buf, r.meta...)
	buf = appendUint32(buf, len(r.entries))
	for i := range r.entries {
		entry := &r.entries[i]
		buf = append(buf, byte(entry.kind), byte(entry.target))
		buf = appendUint32(buf, len(entry.key))
		buf = append(buf, entry.key...)
		buf = binary.LittleEndian.AppendUint64(buf, uint64(entry.offset))
		buf = appendUint32(buf, len(entry.values))
		for _, value := range entry.values {
			buf = appendUint32(buf, len(value))
			buf = append(buf, value...)
		}
	}
	payload := buf[start+8:]
	binary.LittleEndian.PutUint32(buf[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[start+4:], crc32.ChecksumIEEE(payload))
	return buf
}

// growReader reads the fields of a payload, and remembers that one of them
// was not there.
type growReader struct {
	raw   []byte
	short bool
}

func (r *growReader) take(n int) []byte {
	if r.short || n < 0 || n > len(r.raw) {
		r.short = true
		return nil
	}
	out := r.raw[:n:n]
	r.raw = r.raw[n:]
	return out
}

func (r *growReader) uint32() int {
	raw := r.take(4)
	if raw == nil {
		return 0
	}
	return int(binary.LittleEndian.Uint32(raw))
}

func (r *growReader) uint64() uint64 {
	raw := r.take(8)
	if raw == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(raw)
}

// decodeGrowRecord reads a record from its payload, which its checksum has
// vouched for: one that does not parse was not written by this code.
func decodeGrowRecord(payload []byte) (growRecord, error) {
	reader := &growReader{raw: payload}
	record := growRecord{meta: reader.take(reader.uint32())}
	count := reader.uint32()
	for i := 0; i < count && !reader.short; i++ {
		head := reader.take(2)
		if head == nil {
			break
		}
		entry := growEntry{kind: growEntryKind(head[0]), target: growTarget(head[1])}
		entry.key = reader.take(reader.uint32())
		entry.offset = int64(reader.uint64())
		values := reader.uint32()
		for v := 0; v < values && !reader.short; v++ {
			entry.values = append(entry.values, reader.take(reader.uint32()))
		}
		if entry.kind < growEntryPut || entry.kind > growEntryFile || entry.target < growTargetPostings ||
			entry.target >= growTargets {
			return record, errors.Errorf("entry %d of a record of the journal is a write of kind %d to %d", i, entry.kind,
				entry.target)
		}
		record.entries = append(record.entries, entry)
	}
	if reader.short || len(reader.raw) != 0 || len(record.entries) != count {
		return record, errors.Errorf("a record of the journal of %d bytes does not hold the %d writes it says", len(payload),
			count)
	}
	return record, nil
}

// readGrowJournal reads the records of a journal, in their order, up to the
// first that is not whole: a process that died while it wrote a record left
// its head, or a part of it, and that step did not happen. A journal that is
// not there holds none.
func readGrowJournal(path string) ([]growRecord, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read the journal")
	}
	var records []growRecord
	for len(raw) >= 8 {
		size := int(binary.LittleEndian.Uint32(raw))
		if size > len(raw)-8 {
			break
		}
		payload := raw[8 : 8+size]
		if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(raw[4:]) {
			break
		}
		record, err := decodeGrowRecord(payload)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
		raw = raw[8+size:]
	}
	return records, nil
}

// growSetChange is a value added to a row of the bucket of postings by the
// step that runs, or taken out of it.
type growSetChange struct {
	value []byte
	gone  bool
}

// growJournal is the journal of an index, and the writes of the step that
// runs. It is of whoever writes to the index, one at a time; what reads the
// index next to others reads it between two steps, when nothing waits.
type growJournal struct {
	file *os.File
	size int64
	// depth counts the steps that are open, one inside another: the writes
	// of all of them are one record, written when the outermost ends.
	depth   int
	entries []growEntry
	// What the step has written, for it to read back: the last write of
	// every key of a bucket of strategy Replace, nil for a key it deleted;
	// the changes to every row of the bucket of postings, in their order;
	// and the writes to every file, by where they start.
	replaced [growTargets]map[string][]byte
	rows     map[string][]growSetChange
	chunks   [growTargets]map[int64][]byte
	// meta is the meta of the last record.
	meta []byte
	buf  []byte

	// records and bytes count what was appended since the index was opened.
	records, bytes int64
	// crash, in the tests, is called at every point of the end of a step a
	// process can die at: before its record is written, after it, and after
	// each of its writes is handed to its bucket or its file.
	crash func(point string, written int)
}

// active tells whether a step runs, and its writes wait.
func (j *growJournal) active() bool { return j != nil && j.depth > 0 }

func (j *growJournal) reset() {
	j.entries = j.entries[:0]
	for target := range j.replaced {
		clear(j.replaced[target])
		clear(j.chunks[target])
	}
	clear(j.rows)
}

// stage keeps a write of the step that runs.
func (j *growJournal) stage(entry growEntry) {
	j.entries = append(j.entries, entry)
}

// growBucket is a bucket of the index. What a step writes to it waits in
// the journal until the step ends, and the step reads it from there. With
// no journal, and for everything else a bucket does, it is the bucket.
type growBucket struct {
	*lsmkv.Bucket
	g      *growIndex
	target growTarget
}

// bucketOf wraps a bucket of the index, nil for one it does not have.
func (g *growIndex) bucketOf(target growTarget, bucket *lsmkv.Bucket) *growBucket {
	if bucket == nil {
		return nil
	}
	return &growBucket{Bucket: bucket, g: g, target: target}
}

// write hands a write of a bucket or a file to the journal: to the step that
// runs, or, with none, as a step of its own. It tells whether it took it:
// with no journal the caller writes.
func (g *growIndex) write(entry growEntry) (bool, error) {
	j := g.journal
	if j == nil {
		return false, nil
	}
	if j.depth == 0 {
		return true, g.step(func() error {
			_, err := g.write(entry)
			return err
		})
	}
	entry.key = bytes.Clone(entry.key)
	for i, value := range entry.values {
		entry.values[i] = bytes.Clone(value)
	}
	switch entry.kind {
	case growEntryPut:
		if j.replaced[entry.target] == nil {
			j.replaced[entry.target] = make(map[string][]byte)
		}
		j.replaced[entry.target][string(entry.key)] = entry.values[0]
	case growEntryDelete:
		if j.replaced[entry.target] == nil {
			j.replaced[entry.target] = make(map[string][]byte)
		}
		j.replaced[entry.target][string(entry.key)] = nil
	case growEntrySetAdd, growEntrySetDelete:
		if j.rows == nil {
			j.rows = make(map[string][]growSetChange)
		}
		key := string(entry.key)
		for _, value := range entry.values {
			j.rows[key] = append(j.rows[key], growSetChange{value: value, gone: entry.kind == growEntrySetDelete})
		}
	case growEntryFile:
		if j.chunks[entry.target] == nil {
			j.chunks[entry.target] = make(map[int64][]byte)
		}
		j.chunks[entry.target][entry.offset] = entry.values[0]
	}
	j.stage(entry)
	return true, nil
}

func (b *growBucket) Put(key, value []byte) error {
	took, err := b.g.write(growEntry{kind: growEntryPut, target: b.target, key: key, values: [][]byte{value}})
	if took || err != nil {
		return err
	}
	return b.Bucket.Put(key, value)
}

func (b *growBucket) Delete(key []byte) error {
	took, err := b.g.write(growEntry{kind: growEntryDelete, target: b.target, key: key})
	if took || err != nil {
		return err
	}
	return b.Bucket.Delete(key)
}

func (b *growBucket) Get(key []byte) ([]byte, error) {
	if j := b.g.journal; j.active() {
		if value, written := j.replaced[b.target][string(key)]; written {
			return value, nil
		}
	}
	return b.Bucket.Get(key)
}

func (b *growBucket) SetAdd(key []byte, values [][]byte) error {
	took, err := b.g.write(growEntry{kind: growEntrySetAdd, target: b.target, key: key, values: append([][]byte{}, values...)})
	if took || err != nil {
		return err
	}
	return b.Bucket.SetAdd(key, values)
}

func (b *growBucket) SetDeleteSingle(key, value []byte) error {
	took, err := b.g.write(growEntry{kind: growEntrySetDelete, target: b.target, key: key, values: [][]byte{value}})
	if took || err != nil {
		return err
	}
	return b.Bucket.SetDeleteSingle(key, value)
}

// SetList is the row as it is with what the step that runs did to it: a
// value it took out is not there, wherever it was, and those it added come
// after the others, as the store gives them.
func (b *growBucket) SetList(key []byte) ([][]byte, error) {
	values, err := b.Bucket.SetList(key)
	if err != nil {
		return nil, err
	}
	j := b.g.journal
	if !j.active() {
		return values, nil
	}
	changes := j.rows[string(key)]
	if len(changes) == 0 {
		return values, nil
	}
	values = append([][]byte{}, values...)
	for _, change := range changes {
		for i := 0; i < len(values); i++ {
			if bytes.Equal(values[i], change.value) {
				values = append(values[:i], values[i+1:]...)
				i--
			}
		}
		if !change.gone {
			values = append(values, change.value)
		}
	}
	return values, nil
}

// SetRawList is the row as the store holds it, with what the step that runs
// did to it after it: an entry for every value it added and for every one it
// took out, which is what the store will hold of them.
func (b *growBucket) SetRawList(key []byte) ([][]byte, error) {
	values, err := b.Bucket.SetRawList(key)
	if err != nil {
		return nil, err
	}
	j := b.g.journal
	if !j.active() {
		return values, nil
	}
	changes := j.rows[string(key)]
	if len(changes) == 0 {
		return values, nil
	}
	values = append([][]byte{}, values...)
	for _, change := range changes {
		values = append(values, change.value)
	}
	return values, nil
}

// writeFile writes to one of the files of the index, through the journal if
// it has one.
func (g *growIndex) writeFile(target growTarget, raw []byte, offset int64) error {
	took, err := g.write(growEntry{kind: growEntryFile, target: target, values: [][]byte{raw}, offset: offset})
	if took || err != nil {
		return err
	}
	_, err = g.fileOf(target).WriteAt(raw, offset)
	return err
}

// readFile reads from one of the files of the index what was written at the
// offset in one write: what the step that runs wrote there, or the file.
func (g *growIndex) readFile(target growTarget, raw []byte, offset int64) error {
	if j := g.journal; j.active() {
		if written, found := j.chunks[target][offset]; found && len(written) == len(raw) {
			copy(raw, written)
			return nil
		}
	}
	_, err := g.fileOf(target).ReadAt(raw, offset)
	return err
}

func (g *growIndex) fileOf(target growTarget) *os.File {
	switch target {
	case growTargetVectors:
		return g.vectors
	case growTargetSlots:
		return g.slots
	case growTargetBuilt:
		return g.built
	case growTargetCentroids:
		return g.centroids
	default:
		// A bucket, or no target at all.
		return nil
	}
}

func (g *growIndex) storeOf(target growTarget) *growBucket {
	switch target {
	case growTargetPostings:
		return g.postings
	case growTargetMap:
		return g.posts
	case growTargetRouting:
		return g.routing
	case growTargetDists:
		return g.dists
	case growTargetDeleted:
		return g.marks
	case growTargetEdges:
		return g.edges
	case growTargetPending:
		return g.waiting
	default:
		// A file, or no target at all.
		return nil
	}
}

// hand gives a write of a record to its bucket or its file. It is how a step
// ends and how a journal is read again: writing twice what was written
// already leaves it as it was.
func (g *growIndex) hand(entry *growEntry) error {
	if entry.kind == growEntryFile {
		file := g.fileOf(entry.target)
		if file == nil || len(entry.values) != 1 {
			return errors.Errorf("the journal writes to the file %d, which the index does not have", entry.target)
		}
		_, err := file.WriteAt(entry.values[0], entry.offset)
		return errors.Wrapf(err, "write to the file %d", entry.target)
	}
	bucket := g.storeOf(entry.target)
	if bucket == nil {
		return errors.Errorf("the journal writes to the bucket %d, which the index does not have", entry.target)
	}
	var err error
	switch {
	case entry.kind == growEntryDelete:
		err = bucket.Bucket.Delete(entry.key)
	case entry.kind == growEntrySetAdd:
		err = bucket.Bucket.SetAdd(entry.key, entry.values)
	case len(entry.values) != 1:
		err = errors.Errorf("a write of kind %d with %d values", entry.kind, len(entry.values))
	case entry.kind == growEntryPut:
		err = bucket.Bucket.Put(entry.key, entry.values[0])
	case entry.kind == growEntrySetDelete:
		err = bucket.Bucket.SetDeleteSingle(entry.key, entry.values[0])
	}
	return errors.Wrapf(err, "write to the bucket %d", entry.target)
}

// step runs a write of the index: everything it writes to the buckets and
// the files of the index is appended to the journal as one record when it
// ends, and handed to them after. A step inside another is part of it. If
// it fails nothing of it was written, and what the index has in memory is
// no longer what is on disk: whoever called stops using the index. Without
// a journal it is the write alone. The caller writes alone.
func (g *growIndex) step(write func() error) error {
	j := g.journal
	if j == nil {
		return write()
	}
	j.depth++
	err := write()
	j.depth--
	if j.depth > 0 {
		return err
	}
	if err != nil {
		j.reset()
		return err
	}
	return g.endStep()
}

// endStep appends the writes of the step that ran to the journal and hands
// them to their buckets and files.
func (g *growIndex) endStep() error {
	j := g.journal
	defer j.reset()
	meta, err := json.Marshal(g.meta)
	if err != nil {
		return err
	}
	if len(j.entries) == 0 && bytes.Equal(meta, j.meta) {
		return nil
	}
	record := growRecord{meta: meta, entries: j.entries}
	j.buf = record.encode(j.buf[:0])
	if j.crash != nil {
		j.crash("before its record", 0)
	}
	// One write, with no buffer of the process in between: the record is in
	// the system, whole, or the step did not happen.
	if _, err := j.file.WriteAt(j.buf, j.size); err != nil {
		return errors.Wrap(err, "append to the journal")
	}
	j.size += int64(len(j.buf))
	j.records++
	j.bytes += int64(len(j.buf))
	j.meta = append(j.meta[:0], meta...)
	if j.crash != nil {
		j.crash("after its record", 0)
	}
	for i := range j.entries {
		if err := g.hand(&j.entries[i]); err != nil {
			return err
		}
		if j.crash != nil {
			j.crash("after a write", i+1)
		}
	}
	if j.size > growJournalLimit {
		return g.sync()
	}
	return nil
}

// emptyJournal forgets the records of the journal: what they tell is in the
// system by its own way. The caller has written the logs of the buckets and
// the meta.
func (g *growIndex) emptyJournal() error {
	j := g.journal
	if j == nil || j.size == 0 {
		return nil
	}
	if j.active() {
		return errors.New("the journal cannot be emptied while a step runs")
	}
	if err := j.file.Truncate(0); err != nil {
		return errors.Wrap(err, "empty the journal")
	}
	j.size = 0
	return nil
}

// graphChanges marks that the graph over the centroids is about to change:
// whoever opens the index before the mark is taken away (sync) builds the
// graph again. Without a journal nothing is marked: such an index is not one
// a crash leaves usable.
func (g *growIndex) graphChanges() error {
	if g.journal == nil || g.graphMarked {
		return nil
	}
	if err := os.WriteFile(filepath.Join(g.dir, growGraphMark), nil, 0o644); err != nil {
		return errors.Wrap(err, "mark that the centroid graph changes")
	}
	g.graphMarked = true
	return nil
}

// graphSynced takes the mark away: the commit log of the graph is flushed,
// and tells the graph the index has.
func (g *growIndex) graphSynced() error {
	if !g.graphMarked {
		return nil
	}
	if err := os.Remove(filepath.Join(g.dir, growGraphMark)); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "take away the mark of the centroid graph")
	}
	g.graphMarked = false
	return nil
}

// replayGrowFiles writes again to the files of an index what the records of
// its journal wrote to them, before the index reads any of them. The files
// of the vectors and of their slots are those of the generation the meta
// names: the journal is emptied before a generation takes over.
func replayGrowFiles(dir string, meta *growMeta, records []growRecord) (err error) {
	names := map[growTarget]string{
		growTargetVectors:   growVectorsName(meta.VectorGen),
		growTargetSlots:     growSlotsName(meta.VectorGen),
		growTargetBuilt:     growBuiltFile,
		growTargetCentroids: diskCentroidFile,
	}
	files := make(map[growTarget]*os.File)
	defer func() {
		for _, file := range files {
			if closed := file.Close(); err == nil {
				err = closed
			}
		}
	}()
	for r := range records {
		for e := range records[r].entries {
			entry := &records[r].entries[e]
			if entry.kind != growEntryFile {
				continue
			}
			name, known := names[entry.target]
			if !known || len(entry.values) != 1 {
				return errors.Errorf("record %d of the journal writes to the file %d", r, entry.target)
			}
			file := files[entry.target]
			if file == nil {
				if file, err = os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, 0o644); err != nil {
					return errors.Wrapf(err, "open %s to write again what the journal wrote to it", name)
				}
				files[entry.target] = file
			}
			if _, err := file.WriteAt(entry.values[0], entry.offset); err != nil {
				return errors.Wrapf(err, "write again to %s what the journal wrote to it", name)
			}
		}
	}
	return nil
}

// growRecovered tells what an index found when it was opened: the records
// of its journal it applied again, and whether it built its graph again.
type growRecovered struct {
	Records      int  `json:"journalRecords"`
	GraphRebuilt bool `json:"graphRebuilt"`
}

// openJournal ends the opening of an index. If its journal held records, or
// its graph was marked as changed, the index is not the one a sync left: the
// graph is built again where it was marked, everything is synced, and the
// journal emptied. Then the journal is opened for the steps to come, if the
// index was asked to keep one, and otherwise removed.
func (g *growIndex) openJournal(ctx context.Context, opts growOpen, replayed int) error {
	path := filepath.Join(g.dir, growJournalFile)
	marked := false
	if _, err := os.Stat(filepath.Join(g.dir, growGraphMark)); err == nil {
		marked = true
	} else if !os.IsNotExist(err) {
		return errors.Wrap(err, "look for the mark of the centroid graph")
	}
	if marked {
		if err := g.rebuildCentroidGraph(ctx, opts); err != nil {
			return err
		}
		g.graphMarked = true
		g.recovered.GraphRebuilt = true
	}
	g.recovered.Records = replayed
	if replayed > 0 || marked {
		if err := g.sync(); err != nil {
			return errors.Wrap(err, "sync the index the journal left")
		}
	}
	if !opts.journal {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "remove the journal")
		}
		return nil
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return errors.Wrap(err, "open the journal")
	}
	// What it told is synced, and what is left of it is a record that was
	// never whole.
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return errors.Wrap(err, "empty the journal")
	}
	meta, err := json.Marshal(g.meta)
	if err != nil {
		_ = file.Close()
		return err
	}
	g.journal = &growJournal{file: file, meta: meta}
	return nil
}

// rebuildCentroidGraph builds the graph over the centroids again: a graph
// of the centroids of the postings that have members, which are the ones
// the graph of an index holds. The commit log of the graph it replaces may
// tell a graph that is ahead of the index or behind it, or a centroid with
// none of its links. The graph that comes out is not the one there was, link
// by link, and a search may enter by other postings than it did.
func (g *growIndex) rebuildCentroidGraph(ctx context.Context, opts growOpen) error {
	old := g.st.centroids
	vectors := old.vectors
	g.st.centroids = nil
	if err := old.shutdown(ctx); err != nil {
		return errors.Wrap(err, "close the centroid graph to build it again")
	}
	dir := filepath.Join(g.dir, diskGraphDir)
	if err := os.RemoveAll(dir); err != nil {
		return errors.Wrap(err, "remove the centroid graph to build it again")
	}
	if old.store != nil {
		// The codes go with the graph: the bucket keeps those of the centroids
		// that were deleted from the graph and not yet taken out of it, which
		// the graph built again does not hold and would never take out.
		if err := os.RemoveAll(filepath.Join(old.store.GetDir(), old.bucket)); err != nil {
			return errors.Wrap(err, "remove the codes of the centroid graph to build it again")
		}
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		return errors.Wrap(err, "make the directory of the centroid graph")
	}
	graph, err := openCentroidGraphWith(ctx, g.logger, dir, vectors, opts.cycles,
		opts.centroidCodes(filepath.Join(g.dir, diskCentroidFile), g.meta.Dims))
	if err != nil {
		return err
	}
	g.st.centroids = graph
	for p, size := range g.size {
		if size == 0 {
			continue
		}
		centroid, err := graph.publishedVector(ctx, uint64(p))
		if err != nil {
			return errors.Wrapf(err, "read the centroid of posting %d to build the graph again", p)
		}
		if err := graph.graph.Add(ctx, uint64(p), centroid); err != nil {
			return errors.Wrapf(err, "add the centroid of posting %d to the graph built again", p)
		}
	}
	return errors.Wrap(graph.graph.Flush(), "flush the centroid graph built again")
}
