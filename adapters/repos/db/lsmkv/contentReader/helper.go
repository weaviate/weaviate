//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package contentReader

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func GetContentReaderFromBytes(t *testing.T, mmap bool, bytes []byte) ContentReader {
	if mmap {
		return NewMMap(bytes)
	} else {
		path := t.TempDir()
		fo, err := os.Create(path + "output.txt")
		require.Nil(t, err)
		defer fo.Close()
		_, err = fo.Write(bytes)
		require.Nil(t, err)

		fi, err := os.Open(path + "output.txt")
		require.Nil(t, err)

		return NewPread(fi, uint64(len(bytes)))
	}
}
