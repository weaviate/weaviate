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

package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeFileSourceRefusesPathOutsideRoot(t *testing.T) {
	src := NewTreeFileSource(t.TempDir())
	for _, relPath := range []string{"../escape.db", "/etc/passwd"} {
		t.Run(relPath, func(t *testing.T) {
			_, err := src.Stat(relPath)
			require.ErrorContains(t, err, "sanitize file path")

			_, err = src.Open(relPath)
			require.ErrorContains(t, err, "sanitize file path")
		})
	}
}

func TestStagedFilePath(t *testing.T) {
	hashOf := func(relPath string) string {
		sum := sha256.Sum256([]byte(relPath))
		return hex.EncodeToString(sum[:])
	}
	dir := "/" + strings.Repeat("d", 100)
	const prefix = "myclass/shard1/lsm/"

	// The name of rel255 is 347 bytes, so a cut to 255 ends inside its bucket.
	bucket251 := "property_" + strings.Repeat("a", 231) + "_searchable"
	rel255 := prefix + bucket251 + "/segment-1730812345678901234.db"
	cut255 := hashOf(rel255) + "-" + bucket251[:190]
	// The name of rel143 is 144 bytes.
	rel143 := prefix + "property_productDescriptionText_searchable/segment-1730812345678901234.l0.s4.db"

	tests := []struct {
		name     string
		relPath  string
		nameMax  int
		wantName string
	}{
		{
			name:     "short relPath",
			relPath:  prefix + "objects/segment-1.db",
			nameMax:  255,
			wantName: hashOf(prefix+"objects/segment-1.db") + "-objects-segment-1.db",
		},
		{name: "cut to 255 inside the bucket", relPath: rel255, nameMax: 255, wantName: cut255},
		{
			name:     "cut to 143 by one byte",
			relPath:  rel143,
			nameMax:  143,
			wantName: hashOf(rel143) + "-property_productDescriptionText_searchable-segment-1730812345678901234.l0.s4.d",
		},
		{name: "limit of the hash keeps the hash alone", relPath: rel255, nameMax: 64, wantName: hashOf(rel255)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StagedFilePath(ShardStaging{Dir: dir, NameMax: tt.nameMax}, tt.relPath)
			require.Equal(t, dir+string(filepath.Separator)+tt.wantName, got)
		})
	}
}

func TestStagedNameMax(t *testing.T) {
	tests := []struct {
		fsNameMax int
		want      int
		wantErr   bool
	}{
		{fsNameMax: 0, want: 255},
		{fsNameMax: -1, want: 255},
		{fsNameMax: 143, want: 143},
		{fsNameMax: 1024, want: 255},
		{fsNameMax: 64, want: 64},
		{fsNameMax: 63, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.fsNameMax), func(t *testing.T) {
			got, err := StagedNameMax(tt.fsNameMax)
			if tt.wantErr {
				require.ErrorContains(t, err, "at most 63 bytes")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
