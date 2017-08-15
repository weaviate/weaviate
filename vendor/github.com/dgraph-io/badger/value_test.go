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
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/y"
	"github.com/stretchr/testify/require"
)

func TestValueBasic(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	y.Check(err)
	defer os.RemoveAll(dir)

	kv, _ := NewKV(getTestOptions(dir))
	defer kv.Close()
	log := &kv.vlog

	entry := &Entry{
		Key:             []byte("samplekey"),
		Value:           []byte("sampleval"),
		Meta:            BitValuePointer,
		CASCounterCheck: 22222,
		casCounter:      33333,
	}
	entry2 := &Entry{
		Key:             []byte("samplekeyb"),
		Value:           []byte("samplevalb"),
		Meta:            BitValuePointer,
		CASCounterCheck: 22225,
		casCounter:      33335,
	}

	b := new(request)
	b.Entries = []*Entry{entry, entry2}

	log.write([]*request{b})
	require.Len(t, b.Ptrs, 2)
	fmt.Printf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	e, err := log.Read(b.Ptrs[0], nil)
	e2, err := log.Read(b.Ptrs[1], nil)

	require.NoError(t, err)
	readEntries := []Entry{e, e2}
	require.EqualValues(t, []Entry{
		{
			Key:             []byte("samplekey"),
			Value:           []byte("sampleval"),
			Meta:            BitValuePointer,
			CASCounterCheck: 22222,
			casCounter:      33333,
		},
		{
			Key:             []byte("samplekeyb"),
			Value:           []byte("samplevalb"),
			Meta:            BitValuePointer,
			CASCounterCheck: 22225,
			casCounter:      33335,
		},
	}, readEntries)
}

func TestValueGC(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20

	kv, _ := NewKV(opt)
	defer kv.Close()

	sz := 32 << 10
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	for i := 0; i < 45; i++ {
		kv.Delete([]byte(fmt.Sprintf("key%d", i)))
	}

	kv.vlog.RLock()
	lf := kv.vlog.files[0]
	kv.vlog.RUnlock()

	//	lf.iterate(0, func(e Entry) bool {
	//		e.print("lf")
	//		return true
	//	})

	kv.vlog.rewrite(lf)
	var item KVItem
	for i := 45; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := item.Value()
		require.NotNil(t, val)
		require.True(t, len(val) == sz, "Size found: %d", len(val))
	}
}

func TestValueGC2(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	opt := getTestOptions(dir)
	opt.ValueLogFileSize = 1 << 20

	kv, _ := NewKV(opt)
	defer kv.Close()

	sz := 32 << 10
	var entries []*Entry
	for i := 0; i < 100; i++ {
		v := make([]byte, sz)
		rand.Read(v[:rand.Intn(sz)])
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	for i := 0; i < 5; i++ {
		kv.Delete([]byte(fmt.Sprintf("key%d", i)))
	}

	entries = entries[:0]
	for i := 5; i < 10; i++ {
		v := []byte(fmt.Sprintf("value%d", i))
		entries = append(entries, &Entry{
			Key:   []byte(fmt.Sprintf("key%d", i)),
			Value: v,
		})
	}
	kv.BatchSet(entries)
	for _, e := range entries {
		require.NoError(t, e.Error, "entry with error: %+v", e)
	}

	kv.vlog.RLock()
	lf := kv.vlog.files[0]
	kv.vlog.RUnlock()

	//	lf.iterate(0, func(e Entry) bool {
	//		e.print("lf")
	//		return true
	//	})

	kv.vlog.rewrite(lf)
	var item KVItem
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := item.Value()
		require.True(t, len(val) == 0, "Size found: %d", len(val))
	}
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := item.Value()
		require.NotNil(t, val)
		require.Equal(t, string(val), fmt.Sprintf("value%d", i))
	}
	for i := 10; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := kv.Get(key, &item); err != nil {
			t.Error(err)
		}
		val := item.Value()
		require.NotNil(t, val)
		require.True(t, len(val) == sz, "Size found: %d", len(val))
	}
}

var (
	k1 = []byte("k1")
	k2 = []byte("k2")
	k3 = []byte("k3")
	v1 = []byte("value1")
	v2 = []byte("value2")
	v3 = []byte("value3")
)

func TestChecksums(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Set up SST with K1=V1
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	require.NoError(t, kv.Set(k1, v1, 0))
	require.NoError(t, kv.Close())

	// Use a vlog with K1=V1 and a (corrupted) K2=V2
	buf := createVlog(t, []*Entry{
		&Entry{Key: k1, Value: v1},
		&Entry{Key: k2, Value: v2},
	})
	buf[len(buf)-1]++ // Corrupt last byte
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "000000.vlog"), buf, 0777))

	// K1 should exist, but K2 shouldn't.
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)
	var item KVItem
	require.NoError(t, kv.Get(k1, &item))
	require.Equal(t, item.Value(), v1)
	ok, err := kv.Exists(k2)
	require.NoError(t, err)
	require.False(t, ok)
	// Write K3 at the end of the vlog.
	require.NoError(t, kv.Set(k3, v3, 0))
	require.NoError(t, kv.Close())

	// The vlog should contain K1 and K3 (K2 was lost when Badger started up
	// last due to checksum failure).
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)
	iter := kv.NewIterator(IteratorOptions{FetchValues: true})
	iter.Seek(k1)
	require.True(t, iter.Valid())
	it := iter.Item()
	require.Equal(t, it.Key(), k1)
	require.Equal(t, it.Value(), v1)
	iter.Next()
	require.True(t, iter.Valid())
	it = iter.Item()
	require.Equal(t, it.Key(), k3)
	require.Equal(t, it.Value(), v3)
	iter.Close()
	require.NoError(t, kv.Close())
}

func TestPartialAppendToValueLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Create skeleton files.
	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	require.NoError(t, kv.Close())

	// Create truncated vlog to simulate a partial append.
	buf := createVlog(t, []*Entry{
		&Entry{Key: k1, Value: v1},
		&Entry{Key: k2, Value: v2},
	})
	buf = buf[:len(buf)-6]
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "000000.vlog"), buf, 0777))

	// Badger should now start up, but with only K1.
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)
	var item KVItem
	require.NoError(t, kv.Get(k1, &item))
	ok, err := kv.Exists(k2)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, item.Key(), k1)
	require.Equal(t, item.Value(), v1)

	// When K3 is set, it should be persisted after a restart.
	require.NoError(t, kv.Set(k3, v3, 0))
	require.NoError(t, kv.Close())
	kv, err = NewKV(getTestOptions(dir))
	require.NoError(t, err)
	checkKeys(t, kv, [][]byte{k1, k3})
	require.NoError(t, kv.Close())
}

func createVlog(t *testing.T, entries []*Entry) []byte {
	dir, err := ioutil.TempDir("", "badger")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	kv, err := NewKV(getTestOptions(dir))
	require.NoError(t, err)
	require.NoError(t, kv.BatchSet(entries))
	require.NoError(t, kv.Close())

	filename := filepath.Join(dir, "000000.vlog")
	buf, err := ioutil.ReadFile(filename)
	require.NoError(t, err)
	return buf
}

func checkKeys(t *testing.T, kv *KV, keys [][]byte) {
	i := 0
	iter := kv.NewIterator(IteratorOptions{})
	for iter.Seek(keys[0]); iter.Valid(); iter.Next() {
		require.Equal(t, iter.Item().Key(), keys[i])
		i++
	}
	require.Equal(t, i, len(keys))
}

func BenchmarkReadWrite(b *testing.B) {
	rwRatio := []float32{
		0.1, 0.2, 0.5, 1.0,
	}
	valueSize := []int{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
	}

	for _, vsz := range valueSize {
		for _, rw := range rwRatio {
			b.Run(fmt.Sprintf("%3.1f,%04d", rw, vsz), func(b *testing.B) {
				var vl valueLog
				dir, err := ioutil.TempDir("", "vlog")
				y.Check(err)
				defer os.RemoveAll(dir)
				err = vl.Open(nil, getTestOptions(dir))
				y.Check(err)
				defer vl.Close()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					e := new(Entry)
					e.Key = make([]byte, 16)
					e.Value = make([]byte, vsz)
					bl := new(request)
					bl.Entries = []*Entry{e}

					var ptrs []valuePointer

					vl.write([]*request{bl})
					ptrs = append(ptrs, bl.Ptrs...)

					f := rand.Float32()
					if f < rw {
						vl.write([]*request{bl})
						ptrs = append(ptrs, bl.Ptrs...)

					} else {
						ln := len(ptrs)
						if ln == 0 {
							b.Fatalf("Zero length of ptrs")
						}
						idx := rand.Intn(ln)
						e, err := vl.Read(ptrs[idx], nil)
						if err != nil {
							b.Fatalf("Benchmark Read: %v", err)
						}
						if len(e.Key) != 16 {
							b.Fatalf("Key is invalid")
						}
						if len(e.Value) != vsz {
							b.Fatalf("Value is invalid")
						}
					}
				}
			})
		}
	}
}
