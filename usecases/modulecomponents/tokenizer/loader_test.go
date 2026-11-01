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

package tokenizer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const blobURL = "https://openaipublic.blob.core.windows.net/encodings/"

// fallbackLoader stands in for the network loader to record whether it was reached.
type fallbackLoader struct{ called bool }

func (f *fallbackLoader) LoadTiktokenBpe(string) (map[string]int, error) {
	f.called = true
	return map[string]int{"fallback": 0}, nil
}

func TestLocalBpeLoader(t *testing.T) {
	// "a"=0, "b"=1 encoded as tiktoken's "<base64-token> <rank>" line format.
	validBpe := []byte("YQ== 0\nYg== 1\n")

	tests := []struct {
		name         string
		file         string // name to write into the dir (empty = write nothing)
		contents     []byte
		request      string
		want         map[string]int
		wantErr      bool
		wantFallback bool
	}{
		{
			name:     "served from directory",
			file:     "cl100k_base.tiktoken",
			contents: validBpe,
			request:  blobURL + "cl100k_base.tiktoken",
			want:     map[string]int{"a": 0, "b": 1},
		},
		{
			name:         "missing file falls through to network loader",
			request:      blobURL + "cl100k_base.tiktoken",
			want:         map[string]int{"fallback": 0},
			wantFallback: true,
		},
		{
			name:     "corrupt file surfaces error, no fallback",
			file:     "cl100k_base.tiktoken",
			contents: []byte("not base64 !!! 0\n"),
			request:  blobURL + "cl100k_base.tiktoken",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.file != "" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, tt.file), tt.contents, 0o600))
			}
			fallback := &fallbackLoader{}
			loader := &localBpeLoader{dir: dir, fallback: fallback}

			ranks, err := loader.LoadTiktokenBpe(tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.False(t, fallback.called, "must not reach the network loader on a corrupt local file")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, ranks)
			assert.Equal(t, tt.wantFallback, fallback.called)
		})
	}
}

func TestLocalBpeLoader_unreadableFileSurfacesError(t *testing.T) {
	// The path exists but is a directory, so os.ReadFile fails with something other
	// than ErrNotExist. That must surface rather than fall back to the network.
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "cl100k_base.tiktoken"), 0o755))
	fallback := &fallbackLoader{}
	loader := &localBpeLoader{dir: dir, fallback: fallback}

	_, err := loader.LoadTiktokenBpe(blobURL + "cl100k_base.tiktoken")
	require.Error(t, err)
	assert.False(t, fallback.called)
}

func TestParseTiktokenBpe(t *testing.T) {
	t.Run("blank lines are skipped", func(t *testing.T) {
		ranks, err := parseTiktokenBpe([]byte("YQ== 0\n\nYg== 1\n"))
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"a": 0, "b": 1}, ranks)
	})

	t.Run("a line without a separator is an error", func(t *testing.T) {
		// a truncated file must surface rather than yield a partial vocabulary.
		_, err := parseTiktokenBpe([]byte("YQ== 0\nnoseparator\n"))
		require.Error(t, err)
	})
}
