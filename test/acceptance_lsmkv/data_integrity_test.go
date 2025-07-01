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

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestLSMKV_ChecksumRoundtrip(t *testing.T) {
	for _, enableChecksumsInitially := range []bool{true, false} {
		t.Run(fmt.Sprintf("enableChecksumsInitially=%v", enableChecksumsInitially), func(t *testing.T) {
			var (
				key     = []byte("primary_key")
				val     = []byte("some_value")
				dataDir = t.TempDir()
			)

			bucket, err := newTestBucket(dataDir, enableChecksumsInitially)
			require.NoError(t, err)

			require.NoError(t, bucket.Put(key, val))

			// verify that you can read the value
			res, err := bucket.Get(key)
			require.NoError(t, err)
			require.Equal(t, val, res)

			// flush the segment to disk
			require.NoError(t, bucket.Shutdown(context.Background()))

			// verify that you can boostrap from the data on disk when checksums are enabled
			bucket, err = newTestBucket(dataDir, true)
			require.NoError(t, err)

			res, err = bucket.Get(key)
			require.Nil(t, err)
			require.Equal(t, val, res)

			require.NoError(t, bucket.Shutdown(context.Background()))
		})
	}
}

func TestLSMKV_ChecksumsCatchCorruptedFiles(t *testing.T) {
	var (
		key     = []byte("primary_key")
		val     = []byte("some_value")
		dataDir = t.TempDir()
	)

	// create a bucket with checksums enabled and flush some data to disk
	bucket, err := newTestBucket(dataDir, true)
	require.NoError(t, err)
	require.NoError(t, bucket.Put(key, val))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Shutdown(context.Background()))

	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	require.Len(t, entries, 3, "segment files should be created")

	segmentPath := path.Join(dataDir, entries[2].Name())
	fileContent, err := os.ReadFile(segmentPath)
	require.NoError(t, err)

	valueOffset := bytes.Index(fileContent, val)
	require.NotEqual(t, -1, valueOffset, "value was not find in the segment file")

	// corrupt the file contents
	fileContent[valueOffset] = 0xFF
	require.NoError(t, os.WriteFile(segmentPath, fileContent, os.ModePerm))

	_, err = newTestBucket(dataDir, true)
	require.ErrorContains(t, err, "invalid checksum")
}

func newTestBucket(dataPath string, checkSumEnabled bool) (*lsmkv.Bucket, error) {
	log, _ := test.NewNullLogger()
	return lsmkv.NewBucketCreator().
		NewBucket(context.Background(), dataPath, "", log, nil,
			cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(),
			lsmkv.WithSegmentsChecksumValidationEnabled(checkSumEnabled),
			lsmkv.WithCalcCountNetAdditions(true),
		)
}
