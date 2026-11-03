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
	"fmt"
	mathrand "math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
)

// TestMemtableTreeAllocationsDoNotRecurse pins that no memtable tree allocates
// from a stack passing through one function twice. A recursive walk would cost
// a heap profile bucket per path through the tree.
func TestMemtableTreeAllocationsDoNotRecurse(t *testing.T) {
	const keyCount = 2000
	rnd := mathrand.New(mathrand.NewSource(42))
	keys := make([][]byte, keyCount)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%016x", rnd.Uint64()))
	}
	tombstone := tombstonedValue(time.Now())

	replace := &binarySearchTree{}
	tombstoned := &binarySearchTree{}
	mapTree := &binarySearchTreeMap[MapPair]{}
	multi := &binarySearchTreeMulti{}
	roaring := &roaringset.BinarySearchTree{}
	for _, key := range keys {
		replace.insert(key, key, [][]byte{key})
		tombstoned.setTombstone(key, tombstone, nil)
		mapTree.insert(key, MapPair{Key: key, Value: key})
		multi.insert(key, []value{{value: key}})
		roaring.Insert(key, roaringset.Insert{Additions: []uint64{1}})
	}

	tests := []struct {
		name string
		run  func()
	}{
		{name: "replace insert", run: func() {
			tree := &binarySearchTree{}
			for _, key := range keys {
				tree.insert(key, key, nil)
			}
		}},
		{name: "replace setTombstone", run: func() {
			tree := &binarySearchTree{}
			for _, key := range keys {
				tree.setTombstone(key, tombstone, nil)
			}
		}},
		{name: "replace get of a tombstoned key", run: func() {
			for _, key := range keys {
				_, _ = tombstoned.get(key)
			}
		}},
		{name: "replace flattenInOrder", run: func() { replace.flattenInOrder() }},
		{name: "replace flattenInOrderRange", run: func() { replace.flattenInOrderRange(keys[0], nil) }},
		{name: "replace countStats", run: func() { replace.countStats() }},
		{name: "map insert", run: func() {
			tree := &binarySearchTreeMap[MapPair]{}
			for _, key := range keys {
				tree.insert(key, MapPair{Key: key, Value: key})
			}
		}},
		{name: "map get", run: func() {
			for _, key := range keys {
				_, _ = mapTree.get(key)
			}
		}},
		{name: "map flattenInOrder", run: func() { mapTree.flattenInOrder() }},
		{name: "multi insert", run: func() {
			tree := &binarySearchTreeMulti{}
			for _, key := range keys {
				tree.insert(key, []value{{value: key}})
			}
		}},
		{name: "multi flattenInOrder", run: func() { multi.flattenInOrder() }},
		{name: "roaringset Insert", run: func() {
			tree := &roaringset.BinarySearchTree{}
			for _, key := range keys {
				tree.Insert(key, roaringset.Insert{Additions: []uint64{1}})
			}
		}},
		{name: "roaringset Get", run: func() {
			for _, key := range keys {
				_, _ = roaring.Get(key)
			}
		}},
		{name: "roaringset FlattenInOrder", run: func() { roaring.FlattenInOrder() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Nil(t, recursingAllocationStack(tt.run))
		})
	}
}

// recursingAllocationStack returns the first heap profile stack that run
// allocated from and that passes through one function twice, or nil.
func recursingAllocationStack(run func()) []string {
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	run()
	runtime.MemProfileRate = prevRate

	records := make([]runtime.MemProfileRecord, 1024)
	for {
		// inuseZero lists the stacks run just allocated from, whose counts
		// MemProfile publishes only after later GC cycles.
		n, ok := runtime.MemProfile(records, true)
		if ok {
			records = records[:n]
			break
		}
		records = make([]runtime.MemProfileRecord, n+1024)
	}

	runName := runtime.FuncForPC(reflect.ValueOf(run).Pointer()).Name()
	for _, record := range records {
		var functions []string
		seen := map[string]bool{}
		fromRun, recursed := false, false
		frames := runtime.CallersFrames(record.Stack())
		for more := true; more; {
			var frame runtime.Frame
			frame, more = frames.Next()
			functions = append(functions, frame.Function)
			fromRun = fromRun || frame.Function == runName
			// a GC assist inside run re-enters runtime.mallocgc
			if strings.HasPrefix(frame.Function, "runtime.") {
				continue
			}
			recursed = recursed || seen[frame.Function]
			seen[frame.Function] = true
		}
		if fromRun && recursed {
			return functions
		}
	}
	return nil
}
