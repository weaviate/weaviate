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

package rest

import (
	"encoding/binary"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

func TestParseInspectParams(t *testing.T) {
	type want struct {
		params inspectParams
		errSub string
	}
	cases := []struct {
		name string
		url  string
		want want
	}{
		{
			name: "ids and collection only",
			url:  "/x?collection=Foo&ids=" + sampleUUID + "," + altUUID,
			want: want{params: inspectParams{
				Collection: "Foo",
				IDs:        []string{sampleUUID, altUUID},
				Limit:      1000,
			}},
		},
		{
			name: "tenant + buckets allowlist + limit",
			url:  "/x?collection=Foo&ids=" + sampleUUID + "&tenant=t1&buckets=property_year,objects&limit=42",
			want: want{params: inspectParams{
				Collection:      "Foo",
				Tenant:          "t1",
				IDs:             []string{sampleUUID},
				BucketAllowlist: []string{"property_year", "objects"},
				Limit:           42,
			}},
		},
		{
			name: "all=true and forwarded sentinel",
			url:  "/x?collection=Foo&ids=" + sampleUUID + "&all=true&_forwarded=1",
			want: want{params: inspectParams{
				Collection: "Foo",
				IDs:        []string{sampleUUID},
				DumpAll:    true,
				Forwarded:  true,
				Limit:      1000,
			}},
		},
		{
			name: "missing collection",
			url:  "/x?ids=" + sampleUUID,
			want: want{errSub: "collection is required"},
		},
		{
			name: "missing ids",
			url:  "/x?collection=Foo",
			want: want{errSub: "ids is required"},
		},
		{
			name: "invalid limit",
			url:  "/x?collection=Foo&ids=" + sampleUUID + "&limit=foo",
			want: want{errSub: "invalid limit"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tc.url, nil)
			got, err := parseInspectParams(r)
			if tc.want.errSub != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want.errSub)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.params, got)
		})
	}
}

func TestSetContainsDocID(t *testing.T) {
	docID := uint64(0xdeadbeef)
	encLE := make([]byte, 8)
	binary.LittleEndian.PutUint64(encLE, docID)

	other := make([]byte, 8)
	binary.LittleEndian.PutUint64(other, 1)

	cases := []struct {
		name   string
		values [][]byte
		want   bool
	}{
		{"hit", [][]byte{other, encLE}, true},
		{"miss", [][]byte{other}, false},
		{"empty", nil, false},
		{"wrong length skipped", [][]byte{{0x01, 0x02}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, setContainsDocID(tc.values, docID))
		})
	}
}

func TestDecodeDocIDAny(t *testing.T) {
	docID := uint64(42)
	be := make([]byte, 8)
	binary.BigEndian.PutUint64(be, docID)
	le := make([]byte, 8)
	binary.LittleEndian.PutUint64(le, docID)

	targets := map[uint64]string{docID: "uuid-a"}

	t.Run("big endian", func(t *testing.T) {
		id, ok := decodeDocIDAny(be, targets)
		assert.True(t, ok)
		assert.Equal(t, docID, id)
	})
	t.Run("little endian", func(t *testing.T) {
		id, ok := decodeDocIDAny(le, targets)
		assert.True(t, ok)
		assert.Equal(t, docID, id)
	})
	t.Run("miss", func(t *testing.T) {
		miss := make([]byte, 8)
		binary.BigEndian.PutUint64(miss, 99)
		_, ok := decodeDocIDAny(miss, targets)
		assert.False(t, ok)
	})
	t.Run("wrong length", func(t *testing.T) {
		_, ok := decodeDocIDAny([]byte{0x01}, targets)
		assert.False(t, ok)
	})
}

func TestPrintableKey(t *testing.T) {
	cases := []struct {
		in   []byte
		want string
	}{
		{[]byte("hello"), "hello"},
		{[]byte{0x00, 0x01}, ""},
		{[]byte{0xff}, ""},
		{nil, ""},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, printableKey(tc.in))
	}
}

func TestMatchingDocIDsInBitmap(t *testing.T) {
	bm := sroar.NewBitmap()
	bm.Set(10)
	bm.Set(20)
	bm.Set(30)

	targets := map[uint64]string{
		10:  "u10",
		20:  "u20",
		999: "u999",
	}
	matches := matchingDocIDsInBitmap(bm, targets)
	got := make([]uint64, 0, len(matches))
	for _, m := range matches {
		got = append(got, m.DocID)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	assert.Equal(t, []uint64{10, 20}, got)

	// nil bitmap returns nil
	assert.Nil(t, matchingDocIDsInBitmap(nil, targets))
}

func TestFilterDumpForDocID(t *testing.T) {
	full := &bucketDump{
		Strategy:  "roaringset",
		Truncated: true,
		Entries: []bucketEntry{
			{Key: "k1", Matches: []dumpMatch{{DocID: 10, UUID: "u10"}, {DocID: 20, UUID: "u20"}}},
			{Key: "k2", Matches: []dumpMatch{{DocID: 30, UUID: "u30"}}},
			{Key: "k3", Matches: []dumpMatch{{DocID: 10, UUID: "u10"}}},
		},
	}

	out := filterDumpForDocID(full, 10)
	require.NotNil(t, out)
	assert.Equal(t, "roaringset", out.Strategy)
	assert.True(t, out.Truncated)
	require.Len(t, out.Entries, 2)
	assert.Equal(t, "k1", out.Entries[0].Key)
	assert.Equal(t, "k3", out.Entries[1].Key)
	for _, e := range out.Entries {
		require.Len(t, e.Matches, 1)
		assert.Equal(t, uint64(10), e.Matches[0].DocID)
	}

	// docID with no matches → empty entries, but still a non-nil dump.
	none := filterDumpForDocID(full, 9999)
	require.NotNil(t, none)
	assert.Empty(t, none.Entries)

	// nil input → nil output.
	assert.Nil(t, filterDumpForDocID(nil, 10))
}

// Sanity check that the response struct shape stays JSON-friendly.
// (Catches cases where a future field with an interface{} type would silently
// break the wire format.)
func TestInspectResponseShape(t *testing.T) {
	rt := reflect.TypeOf(inspectResponse{})
	collection, ok := rt.FieldByName("Collection")
	require.True(t, ok)
	assert.Equal(t, "collection", collection.Tag.Get("json"))
	results, ok := rt.FieldByName("Results")
	require.True(t, ok)
	assert.Equal(t, "results", results.Tag.Get("json"))
}

const (
	sampleUUID = "11111111-2222-3333-4444-555555555555"
	altUUID    = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
)
