//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCNAOnFlush(t *testing.T) {
	ctx := context.Background()
	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)

	_, ok := findFileWithExt(files, ".cna")
	assert.True(t, ok)
}

func TestCreateCNAInit(t *testing.T) {
	// this test deletes the initial cna and makes sure it gets recreated after
	// the bucket is initialized
	ctx := context.Background()
	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".cna")
	require.True(t, ok)

	err = os.RemoveAll(path.Join(dirName, fname))
	require.Nil(t, err)

	files, err = os.ReadDir(dirName)
	require.Nil(t, err)
	_, ok = findFileWithExt(files, ".cna")
	require.False(t, ok, "verify the file is really gone")

	require.Nil(t, b.Shutdown(ctx))

	// now create a new bucket and assert that the file is re-created on init
	_, err = NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	files, err = os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok = findFileWithExt(files, ".cna")
	require.True(t, ok)
}

func TestRepairCorruptedCNAOnInit(t *testing.T) {
	// this test deletes the initial cna and makes sure it gets recreated after
	// the bucket is initialized
	ctx := context.Background()
	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	logger, _ := test.NewNullLogger()

	b, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	require.Nil(t, b.Put([]byte("hello"), []byte("world")))
	require.Nil(t, b.FlushMemtable(ctx))

	files, err := os.ReadDir(dirName)
	require.Nil(t, err)
	fname, ok := findFileWithExt(files, ".cna")
	require.True(t, ok)

	// now corrupt the file by replacing the count value without adapting the checksum
	require.Nil(t, corruptCNAFile(path.Join(dirName, fname), 12345))

	// now create a new bucket and assert that the file is ignored, re-created on
	// init, and the count matches
	b2, err := NewBucket(ctx, dirName, "", logger, nil, WithStrategy(StrategyReplace))
	require.Nil(t, err)

	assert.Equal(t, 1, b2.Count())
}

func findFileWithExt(files []os.DirEntry, ext string) (string, bool) {
	for _, file := range files {
		fname := file.Name()
		if strings.HasSuffix(fname, ext) {
			return fname, true
		}

	}
	return "", false
}

func corruptCNAFile(fname string, corruptValue uint64) error {
	f, err := os.Open(fname)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(data[4:12], corruptValue)

	f, err = os.Create(fname)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return f.Close()
}
