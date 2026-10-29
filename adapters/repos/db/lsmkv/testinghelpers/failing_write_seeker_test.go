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

package testinghelpers

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFailingWriteSeekerReportsAFileLikePosition pins the contract a caller
// asserting on a position depends on. Both segment writers seek to the start to
// patch a header and back to the end afterwards, so a SeekEnd answering from the
// last seek rather than from the furthest byte written would report a position
// short by the whole body.
func TestFailingWriteSeekerReportsAFileLikePosition(t *testing.T) {
	w := &FailingWriteSeeker{}

	n, err := w.Write(make([]byte, 1000))
	require.NoError(t, err)
	require.Equal(t, 1000, n)

	at, err := w.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Zero(t, at)

	n, err = w.Write(make([]byte, 30))
	require.NoError(t, err)
	require.Equal(t, 30, n)

	at, err = w.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.EqualValues(t, 1000, at,
		"SeekEnd must answer from the furthest byte written, not from the header rewrite")

	at, err = w.Seek(-40, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 960, at)

	_, err = w.Seek(-1, io.SeekStart)
	require.Error(t, err, "a seek before the start of the file is not a position")

	_, err = w.Seek(0, 99)
	require.Error(t, err, "an unknown whence must not be read as a successful seek")
}

// TestFailingWriteSeekerDoesNotGrowOnASeek pins the double against the file it
// stands in for: seeking past the end leaves the end where it was, and only a
// write out there moves it. A double that grew on the seek would let a writer
// pass here while reporting a short segment against os.File.
func TestFailingWriteSeekerDoesNotGrowOnASeek(t *testing.T) {
	onDisk, err := os.CreateTemp(t.TempDir(), "seek")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, onDisk.Close()) })

	for _, f := range []io.WriteSeeker{&FailingWriteSeeker{}, onDisk} {
		t.Run(fmt.Sprintf("%T", f), func(t *testing.T) {
			_, err := f.Write(make([]byte, 100))
			require.NoError(t, err)

			_, err = f.Seek(4000, io.SeekStart)
			require.NoError(t, err)

			at, err := f.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.EqualValues(t, 100, at, "a seek past the end must not extend the file")

			_, err = f.Seek(4000, io.SeekStart)
			require.NoError(t, err)
			_, err = f.Write([]byte("x"))
			require.NoError(t, err)

			at, err = f.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.EqualValues(t, 4001, at, "a write past the end does extend it")
		})
	}
}

// TestFailingWriteSeekerFailsWithAnErrorEvenUnset pins that a short write always
// carries an error. A double returning (0, nil) violates io.Writer, and a
// bufio.Writer in front of it reads that as a successful write.
func TestFailingWriteSeekerFailsWithAnErrorEvenUnset(t *testing.T) {
	unset := &FailingWriteSeeker{FailOnWrite: 1}
	n, err := unset.Write([]byte("abc"))
	require.Zero(t, n)
	require.ErrorIs(t, err, ErrDiskFull)

	own := errors.New("my own failure")
	set := &FailingWriteSeeker{FailOnWrite: 1, Err: own}
	_, err = set.Write([]byte("abc"))
	require.ErrorIs(t, err, own)
}

// TestFailingWriteSeekerFailsAtAByteRatherThanACall pins what FailAtByte is for:
// the same byte fails whether the caller emits it in one Write or one at a time.
func TestFailingWriteSeekerFailsAtAByteRatherThanACall(t *testing.T) {
	const failAt = 13

	tests := []struct {
		name  string
		split []int
	}{
		{"one call", []int{30}},
		{"three calls", []int{10, 10, 10}},
		{"one byte at a time", []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &FailingWriteSeeker{FailAtByte: failAt}

			var accepted int
			var err error
			for _, size := range tt.split {
				var n int
				n, err = w.Write(make([]byte, size))
				accepted += n
				if err != nil {
					break
				}
			}

			require.ErrorIs(t, err, ErrDiskFull)
			require.Equal(t, failAt-1, accepted,
				"every byte before the failing one is accepted, whatever the call boundaries")

			at, err := w.Seek(0, io.SeekEnd)
			require.NoError(t, err)
			require.EqualValues(t, failAt-1, at,
				"the accepted bytes move the position, not just the returned count")
		})
	}
}
