/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package badger

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"golang.org/x/net/trace"

	"github.com/dgraph-io/badger/protos"
	"github.com/dgraph-io/badger/table"
	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func TestManifestBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opt := getTestOptions(dir)
	{
		kv, err := NewKV(opt)
		require.NoError(t, err)
		n := 5000
		for i := 0; i < n; i++ {
			if (i % 10000) == 0 {
				fmt.Printf("Putting i=%d\n", i)
			}
			k := []byte(fmt.Sprintf("%16x", rand.Int63()))
			kv.Set(k, k, 0x00)
		}
		kv.Set([]byte("testkey"), []byte("testval"), 0x05)
		kv.validate()
		require.NoError(t, kv.Close())
	}

	kv, err := NewKV(opt)
	require.NoError(t, err)

	var item KVItem
	if err := kv.Get([]byte("testkey"), &item); err != nil {
		t.Error(err)
	}
	require.EqualValues(t, "testval", string(item.Value()))
	require.EqualValues(t, byte(0x05), item.UserMeta())
	require.NoError(t, kv.Close())
}

func key(prefix string, i int) string {
	return prefix + fmt.Sprintf("%04d", i)
}

func buildTestTable(t *testing.T, prefix string, n int) *os.File {
	y.AssertTrue(n <= 10000)
	keyValues := make([][]string, n)
	for i := 0; i < n; i++ {
		k := key(prefix, i)
		v := fmt.Sprintf("%d", i)
		keyValues[i] = []string{k, v}
	}
	return buildTable(t, keyValues)
}

// TODO - Move these to somewhere where table package can also use it.
// keyValues is n by 2 where n is number of pairs.
func buildTable(t *testing.T, keyValues [][]string) *os.File {
	b := table.NewTableBuilder()
	defer b.Close()
	// TODO: Add test for file garbage collection here. No files should be left after the tests here.

	filename := fmt.Sprintf("%s%s%d.sst", os.TempDir(), string(os.PathSeparator), rand.Int63())
	f, err := y.OpenSyncedFile(filename, true)
	if t != nil {
		require.NoError(t, err)
	} else {
		y.Check(err)
	}

	sort.Slice(keyValues, func(i, j int) bool {
		return keyValues[i][0] < keyValues[j][0]
	})
	for i, kv := range keyValues {
		y.AssertTrue(len(kv) == 2)
		err := b.Add([]byte(kv[0]), y.ValueStruct{[]byte(kv[1]), 'A', 0, uint64(i)})
		if t != nil {
			require.NoError(t, err)
		} else {
			y.Check(err)
		}
	}
	f.Write(b.Finish())
	f.Close()
	f, _ = y.OpenSyncedFile(filename, true)
	return f
}

func TestOverlappingKeyRangeError(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	kv, err := NewKV(&opt)
	require.NoError(t, err)

	lh0 := newLevelHandler(kv, 0)
	lh1 := newLevelHandler(kv, 1)
	f := buildTestTable(t, "k", 2)
	t1, err := table.OpenTable(f, table.MemoryMap)
	require.NoError(t, err)
	defer t1.DecrRef()

	done := lh0.tryAddLevel0Table(t1)
	require.Equal(t, true, done)

	cd := compactDef{
		thisLevel: lh0,
		nextLevel: lh1,
		elog:      trace.New("Badger", "Compact"),
	}

	manifest := createManifest()
	lc, err := newLevelsController(kv, &manifest)
	require.NoError(t, err)
	done = lc.fillTablesL0(&cd)
	require.Equal(t, true, done)
	lc.runCompactDef(0, cd)

	f = buildTestTable(t, "l", 2)
	t2, err := table.OpenTable(f, table.MemoryMap)
	require.NoError(t, err)
	defer t2.DecrRef()
	done = lh0.tryAddLevel0Table(t2)
	require.Equal(t, true, done)

	cd = compactDef{
		thisLevel: lh0,
		nextLevel: lh1,
		elog:      trace.New("Badger", "Compact"),
	}
	lc.fillTablesL0(&cd)
	lc.runCompactDef(0, cd)
}

func TestManifestRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	deletionsThreshold := 10
	mf, m, err := helpOpenOrCreateManifestFile(dir, deletionsThreshold)
	defer func() {
		if mf != nil {
			mf.close()
		}
	}()
	require.NoError(t, err)
	require.Equal(t, 0, m.Creations)
	require.Equal(t, 0, m.Deletions)

	err = mf.addChanges(protos.ManifestChangeSet{[]*protos.ManifestChange{
		makeTableCreateChange(0, 0),
	}})
	require.NoError(t, err)

	for i := uint64(0); i < uint64(deletionsThreshold*3); i++ {
		ch := []*protos.ManifestChange{
			makeTableCreateChange(i+1, 0),
			makeTableDeleteChange(i),
		}
		err := mf.addChanges(protos.ManifestChangeSet{ch})
		require.NoError(t, err)
	}
	err = mf.close()
	require.NoError(t, err)
	mf = nil
	mf, m, err = helpOpenOrCreateManifestFile(dir, deletionsThreshold)
	require.NoError(t, err)
	require.Equal(t, map[uint64]TableManifest{
		uint64(deletionsThreshold * 3): TableManifest{Level: 0},
	}, m.Tables)
}
