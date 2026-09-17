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

package roaringset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
)

// TestDecodesBitmapWrittenByAnOlderSroar reads a bitmap serialized by sroar
// v0.0.16 and asserts the values still come back. Roaring-set segments are
// copied between nodes verbatim by replica movement, backup and restore, and
// shard file copy, so a node running a newer sroar decodes bytes an older one
// wrote. The fixture is a recording, never regenerated: regenerating it under
// the build being tested would assert only that sroar agrees with itself.
func TestDecodesBitmapWrittenByAnOlderSroar(t *testing.T) {
	buf, err := os.ReadFile(filepath.Join("testdata", "bitmap-sroar-v0.0.16.bin"))
	require.NoError(t, err)

	want := make([]uint64, 0, 1011)
	for id := uint64(0); id < 1000; id++ {
		want = append(want, id)
	}
	for id := uint64(70000); id < 70010; id++ {
		want = append(want, id)
	}
	want = append(want, 200000)

	require.Equal(t, want, sroar.FromBuffer(buf).ToArray())
}
