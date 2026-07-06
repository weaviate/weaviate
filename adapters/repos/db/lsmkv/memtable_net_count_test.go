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
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestMemtable_NetCount(t *testing.T) {
	type op = func(*Memtable) error
	put := func(key string) op {
		return func(m *Memtable) error { return m.put([]byte(key), []byte("value")) }
	}
	del := func(key string) op {
		return func(m *Memtable) error { return m.setTombstone([]byte(key)) }
	}
	delWith := func(key string) op {
		return func(m *Memtable) error { return m.setTombstoneWith([]byte(key), time.Now()) }
	}

	tests := []struct {
		name     string
		ops      []op
		expected int
	}{
		{"single insert", []op{put("a")}, 1},
		{"distinct inserts", []op{put("a"), put("b")}, 2},
		{"update within memtable", []op{put("a"), put("a")}, 1},
		{"insert then delete", []op{put("a"), del("a")}, 0},
		{"delete of unseen key", []op{del("a")}, -1},
		{"repeated delete", []op{del("a"), del("a")}, -1},
		{"delete then resurrect", []op{del("a"), put("a")}, 0},
		{"delete with time", []op{put("a"), delWith("a")}, 0},
		{"delete with time of unseen key", []op{delWith("a")}, -1},
		{"mixed", []op{put("a"), put("b"), del("a"), put("c"), del("d")}, 1},
	}

	logger, _ := test.NewNullLogger()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cl, err := newCommitLogger(dir, StrategyReplace, 0)
			require.NoError(t, err)

			m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
				path:     path.Join(dir, "will-never-flush"),
				strategy: StrategyReplace,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, m.commitlog.close())
			})

			for _, apply := range tt.ops {
				require.NoError(t, apply(m))
			}

			require.Equal(t, tt.expected, m.netCount())
		})
	}
}
