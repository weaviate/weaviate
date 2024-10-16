package queue

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.Out = io.Discard

	dir := t.TempDir()
	// test the encoder
	enc, err := NewEncoderWith(dir, logger, 9*50)
	require.NoError(t, err)

	// encode 120 records
	for i := 0; i < 120; i++ {
		err := enc.Encode(uint8(i), uint64(i+1))
		require.NoError(t, err)
	}

	// check the number of files
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	// check if the entry name matches the regex pattern
	require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
	require.Equal(t, "chunk.bin.partial", entries[2].Name())

	// check the content of the files
	checkContent := func(fName string, length int, start, end int) {
		f, err := os.Open(filepath.Join(dir, fName))
		require.NoError(t, err)
		defer f.Close()

		content, err := io.ReadAll(f)
		require.NoError(t, err)

		require.Len(t, content, length)

		if length == 0 {
			return
		}

		f.Seek(0, io.SeekStart)
		buf := bufio.NewReader(f)

		for i := start; i < end; i++ {
			op, key, err := Decode(buf)
			require.NoError(t, err)

			require.Equal(t, uint8(i), op)
			require.Equal(t, uint64(i+1), key)
		}
	}

	checkContent(entries[0].Name(), 9*50, 0, 49)
	checkContent(entries[1].Name(), 9*50, 50, 99)

	// partial file should have 0 records because it was not flushed
	checkContent(entries[2].Name(), 0, 100, 119)

	// flush the encoder
	err = enc.Flush()
	require.NoError(t, err)

	// check the content of the partial file
	checkContent(entries[2].Name(), 9*20, 100, 119)

	// check the queue size
	size := enc.RecordCount()
	require.EqualValues(t, 120, size)

	// check the queue size with a different encoder
	enc2, err := NewEncoderWith(dir, logger, 9*50)
	require.NoError(t, err)
	size = enc2.RecordCount()
	require.EqualValues(t, 120, size)

	// promote the partial file
	err = enc.promoteChunk()
	require.NoError(t, err)

	// check the number of files
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)

	require.Len(t, entries, 3)
	require.Regexp(t, `chunk-\d+\.bin`, entries[0].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[1].Name())
	require.Regexp(t, `chunk-\d+\.bin`, entries[2].Name())

	// check the content of the 3rd file
	checkContent(entries[2].Name(), 9*20, 100, 119)

	// check the queue size again
	size = enc.RecordCount()
	require.EqualValues(t, 120, size)

	// promote again, no-op
	err = enc.promoteChunk()
	require.NoError(t, err)
}
