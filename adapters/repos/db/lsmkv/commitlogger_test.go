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

package lsmkv

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkCommitlogWriter(b *testing.B) {
	for _, val := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", val), func(b *testing.B) {
			cl, err := newCommitLogger(b.TempDir(), "n/a", 0)
			require.NoError(b, err)

			data := make([]byte, val)
			for i := 0; i < len(data); i++ {
				data[i] = byte(rand.Intn(100))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := cl.writeEntry(CommitTypeReplace, data)
				require.NoError(b, err)
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("no space left on device")
}

// close releases the file whether or not the buffered writes reach it.
func TestCommitLogger_CloseReleasesFile(t *testing.T) {
	tests := []struct {
		name        string
		failFlush   bool
		expectError bool
	}{
		{name: "flush succeeds"},
		{name: "flush fails", failFlush: true, expectError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl, err := newCommitLogger(filepath.Join(t.TempDir(), "segment"), StrategyReplace, 0)
			require.NoError(t, err)
			if tt.failFlush {
				cl.writer = bufio.NewWriter(failingWriter{})
			}
			require.NoError(t, cl.writeEntry(CommitTypeReplace, []byte("value")))

			err = cl.close()
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.ErrorIs(t, cl.file.Close(), os.ErrClosed)
		})
	}
}
