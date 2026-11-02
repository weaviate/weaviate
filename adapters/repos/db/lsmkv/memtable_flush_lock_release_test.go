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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFlushReleasesTheMemtableLockOnPanic pins that a panic inside a flush's
// locked read still releases the read lock. The cycle manager recovers a flush
// panic, so the process runs on with whatever lock state the panic left.
//
// The trigger is the test's own. newMemtable fills these fields for every
// strategy, so only a zero-value Memtable makes the walks panic, and no
// production caller reaches that state.
func TestFlushReleasesTheMemtableLockOnPanic(t *testing.T) {
	tests := []struct {
		name  string
		flush func(m *Memtable)
	}{
		{
			name:  "roaring set range nodes",
			flush: func(m *Memtable) { m.flushDataRoaringSetRange(discardingSegmentFile()) },
		},
		{
			name:  "map flatten",
			flush: func(m *Memtable) { m.flushDataMap(discardingSegmentFile()) },
		},
		{
			name:  "inverted flatten",
			flush: func(m *Memtable) { m.flushDataInverted(nil, nil, nil) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Memtable{}

			require.Panics(t, func() { tt.flush(m) },
				"the fixture must reach the locked walk, or the release below proves nothing")

			require.True(t, m.TryLock(),
				"the flush left the memtable read-locked, so a panic skipped its deferred unlock")
			m.Unlock()
		})
	}
}
