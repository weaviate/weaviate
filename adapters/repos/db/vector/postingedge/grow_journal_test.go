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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// The journal gives back the records it was given, whole and in their
// order. Of a record that a process did not finish writing it gives nothing,
// wherever the writing stopped, and the ones before it all the same; so with
// one whose bytes are not the ones that were written.
func TestGrowJournalRecords(t *testing.T) {
	t.Parallel()
	records := []growRecord{
		{meta: []byte(`{"live":1}`), entries: []growEntry{
			{kind: growEntryPut, target: growTargetMap, key: []byte{0, 0, 0, 7}, values: [][]byte{{1, 2, 3, 4}}},
			{kind: growEntrySetAdd, target: growTargetPostings, key: []byte("row"), values: [][]byte{[]byte("one"), {}, []byte("three")}},
			{kind: growEntryFile, target: growTargetVectors, values: [][]byte{make([]byte, 30)}, offset: 1 << 40},
		}},
		{meta: []byte(`{"live":2}`)},
		{meta: []byte(`{"live":3}`), entries: []growEntry{
			{kind: growEntryDelete, target: growTargetPending, key: []byte{9}},
			{kind: growEntrySetDelete, target: growTargetPostings, key: []byte("row"), values: [][]byte{[]byte("one")}},
			{kind: growEntryPut, target: growTargetDeleted, key: []byte{0, 0, 0, 1}, values: [][]byte{{1}}},
		}},
	}
	// same compares what was read with what was written: a value of no
	// bytes comes back as one of no bytes, whichever of the two it was.
	same := func(t *testing.T, want, got []growRecord) {
		t.Helper()
		require.Len(t, got, len(want))
		for r := range want {
			require.Equal(t, string(want[r].meta), string(got[r].meta), "record %d", r)
			require.Len(t, got[r].entries, len(want[r].entries), "record %d", r)
			for e, entry := range want[r].entries {
				read := got[r].entries[e]
				require.Equal(t, entry.kind, read.kind, "record %d, entry %d", r, e)
				require.Equal(t, entry.target, read.target, "record %d, entry %d", r, e)
				require.Equal(t, string(entry.key), string(read.key), "record %d, entry %d", r, e)
				require.Equal(t, entry.offset, read.offset, "record %d, entry %d", r, e)
				require.Len(t, read.values, len(entry.values), "record %d, entry %d", r, e)
				for v := range entry.values {
					require.Equal(t, string(entry.values[v]), string(read.values[v]), "record %d, entry %d, value %d", r, e, v)
				}
			}
		}
	}
	var whole []byte
	var ends []int
	for r := range records {
		whole = records[r].encode(whole)
		ends = append(ends, len(whole))
	}
	path := filepath.Join(t.TempDir(), growJournalFile)

	none, err := readGrowJournal(path)
	require.NoError(t, err)
	require.Empty(t, none, "a journal that is not there")

	require.NoError(t, os.WriteFile(path, whole, 0o644))
	read, err := readGrowJournal(path)
	require.NoError(t, err)
	same(t, records, read)

	t.Run("a record that was not written whole", func(t *testing.T) {
		for size := range len(whole) {
			require.NoError(t, os.WriteFile(path, whole[:size], 0o644))
			read, err := readGrowJournal(path)
			require.NoError(t, err, "%d bytes", size)
			complete := 0
			for complete < len(ends) && ends[complete] <= size {
				complete++
			}
			same(t, records[:complete], read)
		}
	})
	t.Run("a record with other bytes than were written", func(t *testing.T) {
		last := ends[len(ends)-2]
		for at := last; at < len(whole); at += 7 {
			damaged := append([]byte{}, whole...)
			damaged[at] ^= 0x40
			require.NoError(t, os.WriteFile(path, damaged, 0o644))
			read, err := readGrowJournal(path)
			require.NoError(t, err, "byte %d", at)
			same(t, records[:len(records)-1], read)
		}
	})
	t.Run("a record this code did not write", func(t *testing.T) {
		for name, record := range map[string]growRecord{
			"a write of no kind":     {entries: []growEntry{{kind: 0, target: growTargetMap}}},
			"a write to no target":   {entries: []growEntry{{kind: growEntryPut, target: growTargets, values: [][]byte{{1}}}}},
			"a write past the kinds": {entries: []growEntry{{kind: growEntryFileHeld + 1, target: growTargetMap}}},
		} {
			require.NoError(t, os.WriteFile(path, record.encode(nil), 0o644))
			_, err := readGrowJournal(path)
			require.Error(t, err, name)
		}
	})
}

// digest tells everything an index holds, as its buckets and its files have
// it: two indexes with the same digest are the same index. For every posting
// its members in the order of its row, with their codes, their edges and the
// distances of those; for every doc id what the map, the routing codes, the
// marks, the table of slots and the dates of the edges say of it, and its
// vector; the centroids; and the meta. Where the postings carry their halo,
// what the halo of every posting says of every endpoint it has an entry for,
// and not how many entries say it. The graph over the centroids is not in
// it: an index that a crash left builds it again.
func (g *growIndex) digest() (string, error) {
	h := sha256.New()
	meta, err := json.Marshal(g.meta)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(h, "meta %s\n", meta)
	var dp diskPosting
	for p := range uint32(g.meta.Postings) {
		fmt.Fprintf(h, "posting %d retired %v size %d centroid %v\n", p, g.retired[p], g.size[p], g.st.centroids.vectors[p])
		if g.retired[p] {
			continue
		}
		if err := g.fetchMembers(p, &dp); err != nil {
			return "", err
		}
		if err := g.fetchLists(p, &dp); err != nil {
			return "", err
		}
		if err := g.joinLists(p, &dp); err != nil {
			return "", err
		}
		words := g.meta.ScanCodeWords
		for i, docID := range dp.docIDs {
			dists, _, err := g.distsOf(p, docID)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(h, "member %d code %v edges %v dists %v\n", docID, dp.codes[i*words:(i+1)*words], dp.edgesOfMember(i),
				dists)
		}
		if g.haloed() {
			if _, err := g.fetchHalo(p, &dp.halo); err != nil {
				return "", err
			}
			endpoints := slices.Clone(dp.halo.slots)
			slices.Sort(endpoints)
			for _, endpoint := range slices.Compact(endpoints) {
				fmt.Fprintf(h, "halo %d code %v\n", endpoint, dp.halo.find(endpoint))
			}
		}
	}
	var raw []byte
	var vector []float32
	for docID, live := range g.live {
		fmt.Fprintf(h, "doc %d live %v", docID, live)
		if !live {
			fmt.Fprintln(h)
			continue
		}
		if raw, vector, err = g.exact(uint64(docID), raw, vector); err != nil {
			return "", err
		}
		code, _, err := g.routingCode(uint64(docID))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, " posting %d code %v vector %v", g.st.side.postingOf[docID], code, vector)
		if g.deleted != nil {
			fmt.Fprintf(h, " deleted %v", g.deleted[docID])
		}
		if g.builtAt != nil {
			fmt.Fprintf(h, " built %d", g.builtAt[docID])
		}
		fmt.Fprintln(h)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// crashCopy is a copy of the directory of an index as it was at a point of a
// step where a process can die.
type crashCopy struct {
	dir   string
	point string
	wrote int
}

// opened opens a copy of the directory of the fixture's index, as the index
// was opened, checks that it is an index and returns its digest and what it
// found of its journal.
func (f *growFixture) openedCopy(t *testing.T, dir, at string) (string, growRecovered) {
	t.Helper()
	ctx := context.Background()
	c, err := openGrowIndex(ctx, testLogger(), dir, f.provider, f.uc, f.how)
	require.NoError(t, err, at)
	defer func() { require.NoError(t, c.release(ctx), at) }()
	require.NoError(t, c.cleanCentroidGraph(), at)
	require.NoError(t, c.checkCoverage(), at)
	digest, err := c.digest()
	require.NoError(t, err, at)
	return digest, c.recovered
}

// copyNow copies the directory of the fixture's index as it is: what a
// process that died now would leave.
func (f *growFixture) copyNow(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(f.dir)))
	return dir
}

// crashTries tells, of a step of so many writes, whether a crash is tried
// after the given one of them, counted from the first. The crashes before
// and after the record of a step are tried whatever it says.
type crashTries func(writes, wrote int) bool

// crashSample tries a crash after the first write of a step and after the
// last: with the first write in the buckets and the files and the others not,
// and with all of them there, which the journal then hands again. A step of
// one write is tried after it.
func crashSample(writes, wrote int) bool {
	return wrote == 1 || wrote == writes
}

// crashLast tries a crash after the last write of a step alone.
func crashLast(writes, wrote int) bool {
	return wrote == writes
}

// filesHash returns a hash of the names and the bytes of every file under a
// directory.
func filesHash(t *testing.T, dir string) string {
	t.Helper()
	h := sha256.New()
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s %d\n", rel, len(data))
		h.Write(data)
		return nil
	}))
	return hex.EncodeToString(h.Sum(nil))
}

// crashSweep opens the copies a sweep of crashes leaves of the directory of
// the fixture's index. Copies with the same files open as the same index, so
// it opens the files of each copy once and gives the digest and the records
// it read again to every copy with the same files.
type crashSweep struct {
	f    *growFixture
	seen map[string]crashOpened
	// opened counts the copies it opened.
	opened int
}

// crashOpened is what an opened copy was: its digest and the records of the
// journal it read again.
type crashOpened struct {
	digest  string
	records int
}

func newCrashSweep(f *growFixture) *crashSweep {
	return &crashSweep{f: f, seen: map[string]crashOpened{}}
}

// open opens a copy of the directory of the fixture's index, unless one with
// the same files was opened, and returns its digest and how many records of
// its journal it read again.
func (s *crashSweep) open(t *testing.T, dir, at string) (string, int) {
	t.Helper()
	key := filesHash(t, dir)
	if seen, ok := s.seen[key]; ok {
		return seen.digest, seen.records
	}
	digest, recovered := s.f.openedCopy(t, dir, at)
	s.seen[key] = crashOpened{digest: digest, records: recovered.Records}
	s.opened++
	return digest, recovered.Records
}

// crashing runs a step of the fixture's index and checks what a process that
// died at a point of its end would have left: opened, the index is the one
// there was before the step, whose digest is given, if the record of the
// step was not written, and the one there is after it if it was, whatever of
// the step had reached the buckets and the files. tries chooses the writes a
// crash is tried after. It returns the digest of the index after the step
// and how many crashes it checked.
func (s *crashSweep) crashing(t *testing.T, name, before string, tries crashTries, step func() error) (string, int) {
	t.Helper()
	f := s.f
	g := f.g
	require.NotNil(t, g.journal, "the index has a journal")
	var copies []crashCopy
	g.journal.crash = func(point string, wrote int) {
		if point == "after a write" && !tries(len(g.journal.entries), wrote) {
			return
		}
		copies = append(copies, crashCopy{dir: f.copyNow(t), point: point, wrote: wrote})
	}
	err := step()
	g.journal.crash = nil
	require.NoError(t, err, name)
	afterDir := f.copyNow(t)
	after, _ := s.open(t, afterDir, name+", after")
	require.NoError(t, os.RemoveAll(afterDir))
	if len(copies) == 0 {
		// The step wrote nothing.
		require.Equal(t, before, after, name)
		return after, 0
	}
	require.NotEqual(t, before, after, "%s changed the index", name)
	for _, c := range copies {
		at := errors.Errorf("%s, a crash %s (%d written)", name, c.point, c.wrote).Error()
		digest, records := s.open(t, c.dir, at)
		if c.point == "before its record" || c.point == "after a vector" {
			// A vector reaches its file before the record of its step: with
			// no record, the bytes are in a place the index gives out again.
			require.Equal(t, before, digest, "%s: the index is the one before the step", at)
		} else {
			require.Equal(t, after, digest, "%s: the index is the one after the step", at)
			require.Positive(t, records, "%s: the journal was read again", at)
		}
		require.NoError(t, os.RemoveAll(c.dir))
	}
	return after, len(copies)
}

// What a process that dies in the middle of a write leaves on disk is the
// index as it was before that write, or as it is after it: an insert, with
// the split of its posting, a mark of a deleted vector, a step of the pass
// that takes the deleted vectors out. The journal is not emptied between two
// of them unless the case says so, and an index that is opened over it is the
// one all its records tell. The crashes are tried at a sample of the steps
// and of their writes, in two parts that grow the same index apart: the first
// inserts, until one that split its posting and one that did not were tried;
// and, after all the inserts and two marks ran, many marks in one step and
// every step of the pass. The steps that run without their crashes leave
// their records in the journal for the crashes that come after them. Where
// the journal is emptied after every step only the second part runs: the
// first insert of the first part has the journal to itself as well. A copy
// with the same files as one opened before is not opened again (crashSweep).
// TestGrowOpensWhatACrashLeftEveryPoint, of the bench harness, tries every
// step at a dozen points over a larger index.
func TestGrowOpensWhatACrashLeft(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name string
		// syncEvery empties the journal every so many steps, 0 for never.
		syncEvery int
		// halo has the postings carry their halo, and the index no codes in
		// memory.
		halo bool
	}{
		{name: "the journal grows", syncEvery: 0},
		{name: "the journal is emptied after every step", syncEvery: 1},
		{name: "no codes in memory, the journal grows", syncEvery: 0, halo: true},
	}
	for _, tt := range tests {
		for _, deletes := range []bool{false, true} {
			if !deletes && tt.syncEvery > 0 {
				continue
			}
			name := tt.name + ", the inserts"
			if deletes {
				name = tt.name + ", the deletes"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				const total, base, dims, threshold = 170, 150, 32, 24
				format := shardFormat(8)
				if tt.halo {
					format = haloFormat(8, false)
				}
				format.open.journal, format.open.deletes = true, true
				f := newGrowFixtureFormat(t, total, base, dims, false, 64, threshold, format)
				f.rescoring(fewRescored)
				g := f.g
				sweep := newCrashSweep(f)
				steps, tried, checked := 0, 0, 0
				// digest is that of the index as it is when current is set: a
				// step that runs without its crashes leaves it behind.
				digest, current := "", false
				ended := func() {
					if steps++; tt.syncEvery > 0 && steps%tt.syncEvery == 0 {
						require.NoError(t, g.sync())
					}
				}
				// crashed runs a step, tries the crashes of its end after the
				// writes tries chooses and returns how many it checked.
				crashed := func(name string, tries crashTries, step func() error) int {
					if !current {
						dir := f.copyNow(t)
						digest, _ = sweep.open(t, dir, name+", before")
						require.NoError(t, os.RemoveAll(dir))
					}
					var opened int
					digest, opened = sweep.crashing(t, name, digest, tries, step)
					checked += opened
					tried++
					current = true
					ended()
					return opened
				}
				// ran runs a step without trying its crashes: its record goes
				// to the journal all the same.
				ran := func(name string, step func() error) {
					require.NoError(t, step(), name)
					current = false
					ended()
				}
				defer func() {
					t.Logf("%d steps, %d tried, %d crashes checked, %d copies opened, %d records and %d bytes in the journal",
						steps, tried, checked, sweep.opened, g.journal.records, g.journal.bytes)
				}()

				// The inserts are tried, after their last write, until one
				// that split its posting and one that did not were; the others
				// run, in the part of the deletes all of them.
				splits := g.meta.Splits
				splitTried, plainTried := false, false
				for f.inserted < total {
					docID := f.inserted
					f.inserted++
					name := errors.Errorf("the insert of doc %d", docID).Error()
					insert := func() error { return g.insert(ctx, uint64(docID), f.all[docID]) }
					if deletes {
						ran(name, insert)
						continue
					}
					before := g.meta.Splits
					opened := crashed(name, crashLast, insert)
					require.Equal(t, 4, opened, "%s: a crash after its vector reached the file, one before its record, one after "+
						"it and one after its last write", name)
					if g.meta.Splits > before {
						splitTried = true
					} else {
						plainTried = true
					}
					if splitTried && plainTried {
						return
					}
				}
				require.True(t, deletes, "an insert that split its posting and one that did not were tried")
				require.Greater(t, g.meta.Splits, splits+2, "postings split on the way")

				// Marks of deleted vectors, each a step, which run, and many
				// in one step, which is tried after its first write and its
				// last.
				for _, docID := range []uint64{0, 4} {
					ran(errors.Errorf("the mark of doc %d", docID).Error(), func() error {
						_, err := g.remove(docID)
						return err
					})
				}
				opened := crashed("the marks of many docs", crashSample, func() error {
					return g.step(func() error {
						for docID := uint64(20); docID < 150; docID += 2 {
							if _, err := g.remove(docID); err != nil {
								return err
							}
						}
						return nil
					})
				})
				require.Equal(t, 4, opened, "the marks of many docs: a crash before its record, one after it and two after a write")

				// The pass that takes them out, fifteen postings a step: every
				// step is tried, after its last write.
				pass, err := g.beginClean(mergeUnder(g.meta.Threshold), g.meta.Threshold)
				require.NoError(t, err)
				passSteps := 0
				for finished := false; !finished; {
					if pass.gathers() {
						finished, err = pass.step(ctx, cleanGatherStep)
						require.NoError(t, err)
						continue
					}
					passSteps++
					opened := crashed(errors.Errorf("step %d of the pass over the deleted vectors", passSteps).Error(), crashLast, func() error {
						var err error
						finished, err = pass.step(ctx, 15)
						return err
					})
					require.Equal(t, 3, opened, "step %d of the pass: a crash before its record, one after it and one after its last write", passSteps)
				}
				require.Greater(t, passSteps, 1, "the pass took more than one step")
				require.Zero(t, g.deletedCount)
				require.Positive(t, g.cleaned.Merges, "postings were merged")
				if tt.halo {
					require.Positive(t, g.stats.HaloAdded.N, "the writes added to the halos")
					require.Positive(t, g.stats.HaloRows.N, "the halves of a split got their halos")
				}
			})
		}
	}
}

// The record of an insert tells where its vector is and its checksum, not
// its bytes: the vector reaches the file of the vectors before the record
// does the journal. An index opened over what a process that died after the
// record left has the vector, and one opened over a file that does not hold
// it as it was written, which only a machine that died leaves, says how many
// are missing.
func TestGrowJournalTellsOfTheVectorAndDoesNotHoldIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const total, base, dims, threshold = 160, 150, 32, 24
	format := shardFormat(8)
	format.open.journal = true
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, threshold, format)
	f.rescoring(fewRescored)
	g := f.g

	docID := uint64(f.inserted)
	f.inserted++
	require.NoError(t, g.insert(ctx, docID, f.all[docID]))
	records, err := readGrowJournal(filepath.Join(g.dir, growJournalFile))
	require.NoError(t, err)
	require.Len(t, records, 1, "the insert is one record")
	raw := make([]byte, 4*dims)
	for i, x := range f.all[docID] {
		putFloat32(raw[4*i:], x)
	}
	held := 0
	var at int64
	for _, entry := range records[0].entries {
		switch {
		case entry.kind == growEntryFile && entry.target == growTargetVectors:
			require.Fail(t, "the record holds the bytes of the vector")
		case entry.kind == growEntryFileHeld:
			held++
			at = entry.offset
			require.Equal(t, growTargetVectors, entry.target)
			require.Len(t, entry.values, 1)
			require.Len(t, entry.values[0], 8)
			require.Equal(t, uint32(len(raw)), binary.LittleEndian.Uint32(entry.values[0]), "how many bytes")
			require.Equal(t, crc32.ChecksumIEEE(raw), binary.LittleEndian.Uint32(entry.values[0][4:]), "their checksum")
		}
		for _, value := range entry.values {
			require.False(t, bytes.Contains(value, raw), "a write of the record holds the bytes of the vector")
		}
	}
	require.Equal(t, 1, held, "the record tells of the vector once")
	inFile := make([]byte, len(raw))
	_, err = g.vectors.ReadAt(inFile, at)
	require.NoError(t, err)
	require.Equal(t, raw, inFile, "the vector is in the file, where the record says")

	// What a process that died now leaves: the index with the vector.
	after, err := g.digest()
	require.NoError(t, err)
	dir := f.copyNow(t)
	digest, recovered := f.openedCopy(t, dir, "after the insert")
	require.Equal(t, after, digest)
	require.Equal(t, 1, recovered.Records)
	require.Zero(t, recovered.VectorsMissing)

	// What a machine that died before the system wrote the vector leaves:
	// the record, and other bytes in the place of the vector.
	lost := f.copyNow(t)
	file, err := os.OpenFile(filepath.Join(lost, growVectorsName(g.meta.VectorGen)), os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = file.WriteAt(make([]byte, len(raw)), at)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	c, err := openGrowIndex(ctx, testLogger(), lost, f.provider, f.uc, f.how)
	require.NoError(t, err, "the index opens, and says what it does not hold")
	missing := c.recovered.VectorsMissing
	require.NoError(t, c.release(ctx))
	require.Equal(t, 1, missing, "the vector the file does not hold as it was written")
}
