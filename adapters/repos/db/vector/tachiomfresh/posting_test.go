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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// widthsHold checks that every entry of p fits its fields and that the
// array is exactly sized.
func widthsHold(t *testing.T, p packedPosting) {
	t.Helper()
	if p.len() == 0 {
		require.Nil(t, p)
		return
	}
	require.Equal(t, len(p), cap(p), "a posting is exactly sized")
	docBytes, posBytes, _ := p.widths()
	require.Equal(t, 1+p.len()*(docBytes+posBytes), len(p))
	for _, ref := range p.decode() {
		require.LessOrEqual(t, bytesFor(uint64(ref.DocID)), docBytes)
		require.LessOrEqual(t, bytesFor(uint64(ref.Pos)), posBytes)
	}
}

func TestPackedPostingRoundTrip(t *testing.T) {
	tests := []struct {
		name               string
		docBytes, posBytes int
		refs               []tokenRef
	}{
		{name: "empty", docBytes: 4, posBytes: 2},
		{name: "one entry", docBytes: 4, posBytes: 2, refs: []tokenRef{{DocID: 7, Pos: 3}}},
		{name: "1+1, largest values", docBytes: 1, posBytes: 1, refs: []tokenRef{{DocID: 0, Pos: 0}, {DocID: 255, Pos: 255}, {DocID: 128, Pos: 1}}},
		{name: "2+1", docBytes: 2, posBytes: 1, refs: []tokenRef{{DocID: 65535, Pos: 255}, {DocID: 256, Pos: 0}}},
		{name: "3+1", docBytes: 3, posBytes: 1, refs: []tokenRef{{DocID: 16777215, Pos: 9}, {DocID: 65536, Pos: 200}}},
		{name: "4+2, largest values", docBytes: 4, posBytes: 2, refs: []tokenRef{{DocID: math.MaxUint32, Pos: math.MaxUint16}, {DocID: 1, Pos: 256}}},
		{name: "wider than needed", docBytes: 4, posBytes: 2, refs: []tokenRef{{DocID: 1, Pos: 1}, {DocID: 2, Pos: 2}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := encodePosting(tt.refs, tt.docBytes, tt.posBytes, false)
			require.Equal(t, len(tt.refs), p.len())
			require.Equal(t, tt.refs, p.decode())
			widthsHold(t, p)
			if len(tt.refs) > 0 {
				docBytes, posBytes, _ := p.widths()
				require.Equal(t, tt.docBytes, docBytes)
				require.Equal(t, tt.posBytes, posBytes)
			}
			ids := p.docIDs(nil)
			require.Len(t, ids, len(tt.refs))
			for i, ref := range tt.refs {
				require.Equal(t, ref.DocID, ids[i])
			}
		})
	}
}

func TestPackedPostingAppend(t *testing.T) {
	tests := []struct {
		name               string
		docBytes, posBytes int
		refs               []tokenRef
		add                tokenRef
	}{
		{name: "to empty", add: tokenRef{DocID: 70000, Pos: 5}},
		{name: "fits", docBytes: 2, posBytes: 1, refs: []tokenRef{{DocID: 1, Pos: 1}, {DocID: 2, Pos: 2}}, add: tokenRef{DocID: 65535, Pos: 255}},
		{name: "doc id outgrows", docBytes: 2, posBytes: 1, refs: []tokenRef{{DocID: 1, Pos: 1}, {DocID: 65535, Pos: 2}}, add: tokenRef{DocID: 65536, Pos: 3}},
		{name: "position outgrows", docBytes: 1, posBytes: 1, refs: []tokenRef{{DocID: 1, Pos: 255}}, add: tokenRef{DocID: 2, Pos: 256}},
		{name: "both outgrow", docBytes: 1, posBytes: 1, refs: []tokenRef{{DocID: 200, Pos: 200}}, add: tokenRef{DocID: math.MaxUint32, Pos: math.MaxUint16}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p packedPosting
			if len(tt.refs) > 0 {
				p = encodePosting(tt.refs, tt.docBytes, tt.posBytes, false)
			}
			before := p.decode()
			q := p.append(tt.add)
			require.Equal(t, append(before, tt.add), q.decode())
			widthsHold(t, q)
			// copy on write: the old array is untouched
			require.Equal(t, before, p.decode())
			if len(tt.refs) > 0 && bytesFor(uint64(tt.add.DocID)) <= tt.docBytes && bytesFor(uint64(tt.add.Pos)) <= tt.posBytes {
				docBytes, posBytes, _ := q.widths()
				require.Equal(t, tt.docBytes, docBytes, "a fitting entry keeps the widths")
				require.Equal(t, tt.posBytes, posBytes)
			}
		})
	}
}

func TestPackedPostingRemove(t *testing.T) {
	refs := []tokenRef{{DocID: 10, Pos: 0}, {DocID: 20, Pos: 1}, {DocID: 30, Pos: 2}, {DocID: 20, Pos: 3}}
	tests := []struct {
		name    string
		ref     tokenRef
		want    []tokenRef
		removed bool
	}{
		{name: "first", ref: tokenRef{DocID: 10, Pos: 0}, want: []tokenRef{{DocID: 20, Pos: 3}, {DocID: 20, Pos: 1}, {DocID: 30, Pos: 2}}, removed: true},
		{name: "middle", ref: tokenRef{DocID: 30, Pos: 2}, want: []tokenRef{{DocID: 10, Pos: 0}, {DocID: 20, Pos: 1}, {DocID: 20, Pos: 3}}, removed: true},
		{name: "last", ref: tokenRef{DocID: 20, Pos: 3}, want: []tokenRef{{DocID: 10, Pos: 0}, {DocID: 20, Pos: 1}, {DocID: 30, Pos: 2}}, removed: true},
		{name: "same doc, other position", ref: tokenRef{DocID: 20, Pos: 1}, want: []tokenRef{{DocID: 10, Pos: 0}, {DocID: 20, Pos: 3}, {DocID: 30, Pos: 2}}, removed: true},
		{name: "absent doc", ref: tokenRef{DocID: 40, Pos: 0}, want: refs},
		{name: "absent position", ref: tokenRef{DocID: 10, Pos: 1}, want: refs},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := encodePosting(refs, 2, 1, false)
			q, removed := p.remove(tt.ref)
			require.Equal(t, tt.removed, removed)
			require.Equal(t, tt.want, q.decode())
			require.Equal(t, refs, p.decode(), "copy on write")
			widthsHold(t, q)
			if removed {
				docBytes, posBytes, _ := q.widths()
				require.Equal(t, 2, docBytes, "a removal keeps the widths")
				require.Equal(t, 1, posBytes)
			}
		})
	}
	t.Run("the only entry", func(t *testing.T) {
		p := encodePosting(refs[:1], 2, 1, false)
		q, removed := p.remove(refs[0])
		require.True(t, removed)
		require.Nil(t, q)
		require.Equal(t, 0, q.len())
	})
	t.Run("empty", func(t *testing.T) {
		var p packedPosting
		_, removed := p.remove(refs[0])
		require.False(t, removed)
	})
}

func TestWidthsFor(t *testing.T) {
	tests := []struct {
		name               string
		refs               []tokenRef
		docBytes, posBytes int
	}{
		{name: "empty", docBytes: 1, posBytes: 1},
		{name: "small", refs: []tokenRef{{DocID: 0, Pos: 0}, {DocID: 255, Pos: 255}}, docBytes: 1, posBytes: 1},
		{name: "a few thousand documents", refs: []tokenRef{{DocID: 4000, Pos: 172}, {DocID: 10, Pos: 3}}, docBytes: 2, posBytes: 1},
		{name: "below 65,536 documents", refs: []tokenRef{{DocID: 60000, Pos: 178}}, docBytes: 2, posBytes: 1},
		{name: "above 65,535 documents", refs: []tokenRef{{DocID: 65536, Pos: 0}}, docBytes: 3, posBytes: 1},
		{name: "above 256 tokens", refs: []tokenRef{{DocID: 1, Pos: 256}}, docBytes: 1, posBytes: 2},
		{name: "largest", refs: []tokenRef{{DocID: math.MaxUint32, Pos: math.MaxUint16}}, docBytes: 4, posBytes: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docBytes, posBytes, _ := widthsFor(tt.refs)
			require.Equal(t, tt.docBytes, docBytes)
			require.Equal(t, tt.posBytes, posBytes)
			if p := pack(tt.refs); len(tt.refs) > 0 {
				require.Equal(t, 1+len(tt.refs)*(docBytes+posBytes), len(p))
				require.Equal(t, tt.refs, p.decode())
			}
		})
	}
}

func TestPackedPostingDocIDsReusesBuffer(t *testing.T) {
	buf := make([]uint32, 0, 16)
	for _, docBytes := range []int{1, 2, 3, 4} {
		p := encodePosting([]tokenRef{{DocID: 5, Pos: 1}, {DocID: 200, Pos: 2}, {DocID: 3, Pos: 3}}, docBytes, 1, false)
		ids := p.docIDs(buf)
		require.Equal(t, []uint32{5, 200, 3}, ids)
		require.Equal(t, &buf[:1][0], &ids[0], "the buffer is reused")
	}
	// an empty posting gives an empty slice
	require.Empty(t, packedPosting(nil).docIDs(buf))
}

func TestPostingTablePacked(t *testing.T) {
	var table postingTable
	require.Equal(t, 1, table.append(3, tokenRef{DocID: 1, Pos: 0}))
	require.Equal(t, 2, table.append(3, tokenRef{DocID: 70000, Pos: 300}))
	require.Nil(t, table.get(2))
	require.Nil(t, table.get(9))
	require.Equal(t, []tokenRef{{DocID: 1, Pos: 0}, {DocID: 70000, Pos: 300}}, table.get(3).decode())
	require.True(t, table.remove(3, tokenRef{DocID: 1, Pos: 0}))
	require.False(t, table.remove(3, tokenRef{DocID: 1, Pos: 0}))
	require.False(t, table.remove(9, tokenRef{DocID: 1, Pos: 0}))
	require.Equal(t, []tokenRef{{DocID: 70000, Pos: 300}}, table.get(3).decode())
	table.set(5, []tokenRef{{DocID: 2, Pos: 2}})
	require.Equal(t, 1, table.get(5).len())
	widthsHold(t, table.get(5))
	table.set(5, nil)
	require.Equal(t, 0, table.get(5).len())
}

// TestVersionWrapKeepsOldEntriesStale re-inserts one doc id until its
// version wraps. Every re-insert raises the version with
// VersionMap.Reinsert, as AddMulti does (insertVersion), so an entry the
// first insert wrote is stale from the second insert on, until a cleanup
// removes it. The version is 7 bits and wraps from 127 to 1
// (hfresh.VectorVersion.Increment). An entry written at the first version
// must stay stale after the wrap.
//
// It fails today: after the wrap, the document's version is 1 again, and
// live reads every earlier entry still in a posting as live. The version
// map is hfresh's, which has the same defect. The test is skipped until it
// is fixed; remove the Skip to reproduce it.
func TestVersionWrapKeepsOldEntriesStale(t *testing.T) {
	t.Skip("known defect, shared with hfresh: the 7-bit version wraps from 127 to 1, and entries of earlier inserts read as live again")
	tf := newTestIndex(t, testMaxPostingSize, testKCentroids, testKDocsToScore, hfreshEFConstruction)
	defer tf.close(t)
	tf.Index.opts.disableReassign = true

	const docID = 1
	vectors := createTestVectors(testDim, 4)
	addMultiVectorToIndex(t, &tf, docID, vectors)
	tf.Index.waitForMaintenance(t)

	var old tokenRef
	found := false
	for id := range tf.Index.postings.lists {
		for _, ref := range tf.posting(uint64(id)) {
			if ref.DocID == docID {
				old, found = ref, true
			}
		}
	}
	require.True(t, found, "no entry of doc %d", docID)
	require.Equal(t, uint8(1), old.version())

	// the re-inserts up to version 127, the last before the wrap
	for range 126 {
		_, err := tf.Index.VersionMap.Reinsert(t.Context(), docID)
		require.NoError(t, err)
	}
	live, err := tf.Index.live(t.Context(), old)
	require.NoError(t, err)
	require.False(t, live, "an entry of the first insert is stale at version 127")

	// the next re-insert wraps the version to 1
	v, err := tf.Index.VersionMap.Reinsert(t.Context(), docID)
	require.NoError(t, err)
	require.Equal(t, uint8(1), v.Version())
	live, err = tf.Index.live(t.Context(), old)
	require.NoError(t, err)
	require.False(t, live, "after the version wrapped, an entry of the first insert reads live again")
}
