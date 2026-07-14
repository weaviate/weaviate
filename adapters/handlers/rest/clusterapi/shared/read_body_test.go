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

package shared

import (
	"bytes"
	"errors"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadBody(t *testing.T) {
	payload := []byte("internode payload bytes")

	t.Run("known content length", func(t *testing.T) {
		out, err := ReadBody(bytes.NewReader(payload), int64(len(payload)))
		require.NoError(t, err)
		assert.Equal(t, payload, out)
		// exact-size allocation: no spare capacity from growth
		assert.Equal(t, len(payload), cap(out))
	})

	t.Run("small body allocates only its exact buffer", func(t *testing.T) {
		rd := bytes.NewReader(nil)
		allocs := testing.AllocsPerRun(100, func() {
			rd.Reset(payload)
			if _, err := ReadBody(rd, int64(len(payload))); err != nil {
				t.Error(err)
			}
		})
		assert.LessOrEqual(t, allocs, 1.0)
	})

	t.Run("unknown content length falls back to ReadAll", func(t *testing.T) {
		for _, cl := range []int64{-1, 0} {
			out, err := ReadBody(bytes.NewReader(payload), cl)
			require.NoError(t, err)
			assert.Equal(t, payload, out)
		}
	})

	t.Run("empty body with zero content length", func(t *testing.T) {
		out, err := ReadBody(bytes.NewReader(nil), 0)
		require.NoError(t, err)
		assert.Empty(t, out)
	})

	t.Run("body shorter than declared length", func(t *testing.T) {
		_, err := ReadBody(bytes.NewReader(payload), int64(len(payload))+10)
		require.Error(t, err)
		assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got %v", err)
	})

	t.Run("declared length shorter than body reads only declared bytes", func(t *testing.T) {
		// net/http bounds body readers at Content-Length, so this cannot
		// happen with real HTTP bodies; the helper still behaves sanely.
		out, err := ReadBody(bytes.NewReader(payload), 4)
		require.NoError(t, err)
		assert.Equal(t, payload[:4], out)
	})

	t.Run("spoofed huge content length with tiny body errors after small allocation", func(t *testing.T) {
		// a claim alone must not buy more than UnverifiedBodyAlloc: the tiny
		// body fails the head read before any claim-sized buffer exists
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		out, err := ReadBody(bytes.NewReader(payload), 2<<30)
		runtime.ReadMemStats(&after)
		require.Error(t, err)
		assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got %v", err)
		assert.Nil(t, out)
		allocated := after.TotalAlloc - before.TotalAlloc
		assert.Less(t, allocated, uint64(2*UnverifiedBodyAlloc),
			"claim of 2GiB must not allocate beyond the unverified budget, got %d bytes", allocated)
	})

	t.Run("spoofed huge content length with real bytes past head still errors on truncation", func(t *testing.T) {
		// enough real bytes to pass the head read, then truncated mid-body
		partial := bytes.Repeat([]byte{0xEE}, 2*UnverifiedBodyAlloc)
		_, err := ReadBody(bytes.NewReader(partial), 8<<20)
		require.Error(t, err)
		assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got %v", err)
	})

	t.Run("content length above unverified budget with full body is read exactly", func(t *testing.T) {
		body := bytes.Repeat([]byte{0x42}, 2*UnverifiedBodyAlloc+512)
		out, err := ReadBody(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)
		assert.Equal(t, body, out)
		assert.Equal(t, len(body), cap(out))
	})

	t.Run("body larger than on-arrival cap is read fully", func(t *testing.T) {
		big := bytes.Repeat([]byte{0xAB}, MaxOnArrivalBodyAlloc+512)
		out, err := ReadBody(bytes.NewReader(big), int64(len(big)))
		require.NoError(t, err)
		assert.Equal(t, big, out)
	})

	t.Run("content length exactly at cap uses exact-size path", func(t *testing.T) {
		big := bytes.Repeat([]byte{0xCD}, MaxOnArrivalBodyAlloc)
		out, err := ReadBody(bytes.NewReader(big), int64(len(big)))
		require.NoError(t, err)
		assert.Equal(t, big, out)
		assert.Equal(t, MaxOnArrivalBodyAlloc, cap(out))
	})

	t.Run("content length exactly at unverified budget uses exact-size path", func(t *testing.T) {
		body := bytes.Repeat([]byte{0x11}, UnverifiedBodyAlloc)
		out, err := ReadBody(bytes.NewReader(body), int64(len(body)))
		require.NoError(t, err)
		assert.Equal(t, body, out)
		assert.Equal(t, UnverifiedBodyAlloc, cap(out))
	})
}
