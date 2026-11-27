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

package postingedge

import (
	"errors"
	"math"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// A pass that lays the vectors out writes every one of them again next to
// the files the index has, which stay until it ends. It starts only if the
// disk has room for what it writes and a quarter more: one that did not fit
// would fill the disk before it failed, for the inserts and for everything
// else that writes to it. Without room the index stays as it is, whoever
// settles it is told of no error, nothing of the pass is left on the disk,
// and the pass is tried again once the index has grown by a tenth.
func TestPersistentLaysOutOnlyWithRoomOnTheDisk(t *testing.T) {
	t.Parallel()
	const (
		base  = doublingBase
		first = 2*base + 10
		total = first + layoutStep + 10
	)
	tests := []struct {
		name string
		// free gives the bytes free on the disk, from those the pass writes.
		// Without it the disk does not tell.
		free    func(writes uint64) uint64
		laysOut bool
	}{
		{name: "no room at all", free: func(uint64) uint64 { return 0 }},
		{name: "room for its files and no more", free: func(writes uint64) uint64 { return writes }},
		{name: "a byte short", free: func(writes uint64) uint64 { return layoutRoom(writes) - 1 }},
		{name: "just enough", free: layoutRoom, laysOut: true},
		{name: "the disk does not tell", laysOut: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var (
				free  atomic.Uint64
				fails atomic.Bool
				asked atomic.Int64
			)
			free.Store(math.MaxUint64)
			f := newDoublingFixture(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
				cfg.noBackground = true
				cfg.diskFree = func(string) (uint64, error) {
					asked.Add(1)
					if fails.Load() {
						return 0, errors.New("the disk does not tell")
					}
					return free.Load(), nil
				}
			})
			ctx := persistentCtx()
			state := func() (gen, laidOut int, writes uint64, dir string) {
				f.index.mu.RLock()
				defer f.index.mu.RUnlock()
				g := f.index.g
				return g.meta.VectorGen, g.meta.VectorsLaidOut, g.layoutBytes(), g.dir
			}

			// The index doubles: a pass is due for whoever settles it.
			f.add(t, 0, first)
			gen, laidOut, writes, dir := state()
			require.Equal(t, 1, gen)
			require.Equal(t, base, laidOut)
			require.Greater(t, writes, uint64(first*4*len(f.vectors[0])), "the pass writes every vector and the table of their slots")
			if tt.free == nil {
				fails.Store(true)
			} else {
				free.Store(tt.free(writes))
			}
			before := asked.Load()
			require.NoError(t, f.index.Settle(ctx))
			require.Greater(t, asked.Load(), before, "the pass asked for the room of the disk")
			gen, laidOut, _, _ = state()
			if tt.laysOut {
				require.Equal(t, 2, gen)
				require.Equal(t, first, laidOut)
				f.covered(t, "laid out")
				f.vectorsIntact(t, "laid out")
				return
			}
			require.Equal(t, 1, gen, "no pass without room")
			require.Equal(t, base, laidOut)
			require.NoFileExists(t, filepath.Join(dir, growVectorsName(2)), "nothing of the pass is on the disk")
			require.NoFileExists(t, filepath.Join(dir, growSlotsName(2)), "nothing of the pass is on the disk")
			f.covered(t, "with no room")
			f.vectorsIntact(t, "with no room")

			// The disk has room again: the pass is not tried again at once,
			// but once the index has grown by a tenth, or by a step of a pass.
			fails.Store(false)
			free.Store(math.MaxUint64)
			before = asked.Load()
			require.NoError(t, f.index.Settle(ctx))
			require.Equal(t, before, asked.Load(), "not tried again at once")
			gen, _, _, _ = state()
			require.Equal(t, 1, gen)
			f.add(t, first, total)
			require.NoError(t, f.index.Settle(ctx))
			gen, laidOut, _, _ = state()
			require.Equal(t, 2, gen, "tried again once the index has grown")
			require.Equal(t, total, laidOut)
			f.covered(t, "laid out in the end")
			f.vectorsIntact(t, "laid out in the end")
		})
	}
}
