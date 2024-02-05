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

package hnsw

import (
	"context"
	_ "fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type MockDirEntry struct {
	name  string
	isDir bool
}

func (d MockDirEntry) Name() string {
	return d.name
}

func (d MockDirEntry) IsDir() bool {
	return d.isDir
}

func (d MockDirEntry) Type() os.FileMode {
	return os.ModePerm
}

func (d MockDirEntry) Info() (os.FileInfo, error) {
	return nil, nil
}

func TestRemoveTmpScratchOrHiddenFiles(t *testing.T) {
	entries := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: ".nfs6b46801cd962afbc00000005", isDir: false},
		MockDirEntry{name: ".mystery-folder", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
		MockDirEntry{name: "1682473161.scratch.tmp", isDir: false},
	}

	expected := []os.DirEntry{
		MockDirEntry{name: "1682473161", isDir: false},
		MockDirEntry{name: "1682473161.condensed", isDir: false},
	}

	result := removeTmpScratchOrHiddenFiles(entries)

	if len(result) != len(expected) {
		t.Errorf("Expected %d entries, got %d", len(expected), len(result))
	}

	for i, entry := range result {
		if entry.Name() != expected[i].Name() {
			t.Errorf("Expected entry %d to be %s, got %s", i, expected[i].Name(), entry.Name())
		}
	}
}

func getFirstFileFromDir(t *testing.T, dir string) string {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			return filepath.Join(dir, entry.Name())
		}
	}

	return ""
}

func TestCommitLogger(t *testing.T) {
	l := logrus.New()
	l.Out = io.Discard

	dir := t.TempDir()
	cl, err := NewCommitLogger(dir, "test", l, cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	cl.AddNode(&vertex{id: 1, level: 2}) // 11
	cl.AddPQ(ssdhelpers.PQData{          // 10
		Ks:                  1,
		M:                   2,
		Dimensions:          3,
		EncoderType:         ssdhelpers.UseTileEncoder,
		EncoderDistribution: byte(4),
		UseBitsEncoding:     true,
	})
	cl.SetEntryPointWithMaxLayer(1, 2)              // 11
	cl.ReplaceLinksAtLevel(1, 2, []uint64{1, 2, 3}) // 37
	cl.AddLinkAtLevel(1, 2, 3)                      // 19
	cl.AddTombstone(1)                              // 9
	cl.RemoveTombstone(1)                           // 9
	cl.ClearLinks(1)                                // 9
	cl.ClearLinksAtLevel(1, 2)                      // 11
	cl.DeleteNode(1)                                // 9
	cl.Reset()                                      // 1

	err = cl.Flush()
	require.NoError(t, err)

	content, err := os.ReadFile(getFirstFileFromDir(t, filepath.Join(dir, "test.hnsw.commitlog.d")))
	require.NoError(t, err)

	require.Len(t, content, 136)

	// ensure the order of the operations is correct
	require.Equal(t, byte(AddNode), content[0])
	require.Equal(t, byte(AddPQ), content[11])
	require.Equal(t, byte(SetEntryPointMaxLevel), content[21])
	require.Equal(t, byte(ReplaceLinksAtLevel), content[32])
	require.Equal(t, byte(AddLinkAtLevel), content[69])
	require.Equal(t, byte(AddTombstone), content[88])
	require.Equal(t, byte(RemoveTombstone), content[97])
	require.Equal(t, byte(ClearLinks), content[106])
	require.Equal(t, byte(ClearLinksAtLevel), content[115])
	require.Equal(t, byte(DeleteNode), content[126])
	require.Equal(t, byte(ResetIndex), content[135])

	err = cl.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestCommitLoggerConcurrency(t *testing.T) {
	l := logrus.New()
	l.Out = io.Discard

	dir := t.TempDir()
	cl, err := NewCommitLogger(dir, "test", l, cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				cl.AddTombstone(1)
			}
		}()
	}

	wg.Wait()

	err = cl.Flush()
	require.NoError(t, err)

	content, err := os.ReadFile(getFirstFileFromDir(t, filepath.Join(dir, "test.hnsw.commitlog.d")))
	require.NoError(t, err)

	require.Len(t, content, 9*10*1000)
}
