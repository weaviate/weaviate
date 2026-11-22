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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The journal gives back the records it was given, whole and in their
// order. Of a record that a process did not finish writing it gives nothing,
// wherever the writing stopped, and the ones before it all the same; so with
// one whose bytes are not the ones that were written.
func TestGrowJournalRecords(t *testing.T) {
	records := []growRecord{
		{meta: []byte(`{"live":1}`), entries: []growEntry{
			{kind: growEntryPut, target: growTargetMap, key: []byte{0, 0, 0, 7}, values: [][]byte{{1, 2, 3, 4}}},
			{kind: growEntrySetAdd, target: growTargetPostings, key: []byte("row"), values: [][]byte{[]byte("one"), {}, []byte("three")}},
			{kind: growEntryFile, target: growTargetVectors, values: [][]byte{make([]byte, 300)}, offset: 1 << 40},
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
			"a write past the kinds": {entries: []growEntry{{kind: growEntryFile + 1, target: growTargetMap}}},
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

// crashPoints are the writes of a step after which a crash is tried, counted
// from the first: all of the first ones and then fewer and fewer, so that a
// step of a few writes is tried whole and one of hundreds at a dozen places.
var crashPoints = map[int]bool{
	1: true, 2: true, 3: true, 5: true, 8: true, 13: true, 21: true, 34: true, 55: true, 89: true, 144: true, 233: true,
	377: true, 610: true, 987: true,
}

// crashing runs a step of the fixture's index and checks what a process that
// died at a point of its end would have left: opened, the index is the one
// there was before the step, whose digest is given, if the record of the
// step was not written, and the one there is after it if it was, whatever of
// the step had reached the buckets and the files. It returns the digest of
// the index after the step and how many crashes it opened.
func (f *growFixture) crashing(t *testing.T, name, before string, step func() error) (string, int) {
	t.Helper()
	g := f.g
	require.NotNil(t, g.journal, "the index has a journal")
	var copies []crashCopy
	g.journal.crash = func(point string, wrote int) {
		if point == "after a write" && !crashPoints[wrote] {
			return
		}
		copies = append(copies, crashCopy{dir: f.copyNow(t), point: point, wrote: wrote})
	}
	err := step()
	g.journal.crash = nil
	require.NoError(t, err, name)
	after, _ := f.openedCopy(t, f.copyNow(t), name+", after")
	if len(copies) == 0 {
		// The step wrote nothing.
		require.Equal(t, before, after, name)
		return after, 0
	}
	require.NotEqual(t, before, after, "%s changed the index", name)
	for _, c := range copies {
		at := errors.Errorf("%s, a crash %s (%d written)", name, c.point, c.wrote).Error()
		digest, recovered := f.openedCopy(t, c.dir, at)
		if c.point == "before its record" {
			require.Equal(t, before, digest, "%s: the index is the one before the step", at)
		} else {
			require.Equal(t, after, digest, "%s: the index is the one after the step", at)
			require.Positive(t, recovered.Records, "%s: the journal was read again", at)
		}
		require.NoError(t, os.RemoveAll(c.dir))
	}
	return after, len(copies)
}

// What a process that dies in the middle of a write leaves on disk is the
// index as it was before that write, or as it is after it: an insert, with
// the split of its posting, a mark of a deleted vector, a step of the pass
// that takes the deleted vectors out, a refresh of the edges of a vector.
// The journal is not emptied between two of them unless the case says so,
// and an index that is opened over it is the one all its records tell.
func TestGrowOpensWhatACrashLeft(t *testing.T) {
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
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims = 1036, 1000, 64
			format := shardFormat(8)
			if tt.halo {
				format = haloFormat(8, false)
			}
			format.open.journal, format.open.deletes = true, true
			f := newGrowFixtureFormat(t, total, base, dims, false, 64, 46, format)
			g := f.g
			steps, checked := 0, 0
			digest, _ := f.openedCopy(t, f.copyNow(t), "the base")
			// crashed runs a step and tries the crashes of its end.
			crashed := func(name string, step func() error) {
				var opened int
				digest, opened = f.crashing(t, name, digest, step)
				checked += opened
				if steps++; tt.syncEvery > 0 && steps%tt.syncEvery == 0 {
					require.NoError(t, g.sync())
				}
			}
			splits := g.meta.Splits
			for f.inserted < total {
				docID := f.inserted
				f.inserted++
				crashed(errors.Errorf("the insert of doc %d", docID).Error(), func() error {
					return g.insert(ctx, uint64(docID), f.all[docID])
				})
			}
			require.Greater(t, g.meta.Splits, splits+2, "postings split on the way")

			// Marks of deleted vectors, each a step, and several in one.
			for docID := uint64(0); docID < 12; docID += 4 {
				crashed(errors.Errorf("the mark of doc %d", docID).Error(), func() error {
					_, err := g.remove(docID)
					return err
				})
			}
			crashed("the marks of many docs", func() error {
				return g.step(func() error {
					for docID := uint64(100); docID < 700; docID += 2 {
						if _, err := g.remove(docID); err != nil {
							return err
						}
					}
					return nil
				})
			})

			// The pass that takes them out, a few postings a step.
			pass, err := g.beginClean(mergeUnder(g.meta.Threshold), g.meta.Threshold)
			require.NoError(t, err)
			for finished := false; !finished; {
				if pass.gathers() {
					finished, err = pass.step(ctx, cleanGatherStep)
					require.NoError(t, err)
					continue
				}
				crashed(errors.Errorf("step %d of the pass over the deleted vectors", steps).Error(), func() error {
					var err error
					finished, err = pass.step(ctx, 6)
					return err
				})
			}
			require.Zero(t, g.deletedCount)
			require.Positive(t, g.cleaned.Merges, "postings were merged")
			if tt.halo {
				require.Positive(t, g.stats.HaloAdded.N, "the writes added to the halos")
				require.Positive(t, g.stats.HaloRows.N, "the halves of a split got their halos")
			}
			t.Logf("%d steps, %d crashes opened, %d records and %d bytes in the journal", steps, checked, g.journal.records,
				g.journal.bytes)
			assert.Greater(t, checked, 100)
		})
	}
}
