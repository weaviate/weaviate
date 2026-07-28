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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/lsmkv"
)

// slowMemtable pushes a read past the 100ms threshold that gates the
// slow-read debug logging, which is the only branch that touches b.logger.
type slowMemtable struct {
	memtable
	delay time.Duration
}

func (m slowMemtable) get(key []byte) ([]byte, error) {
	time.Sleep(m.delay)
	return m.memtable.get(key)
}

// Returns NotFound rather than delegating: the fixture has no secondary-index
// arrays, and the branch under test runs in a defer regardless of the result.
func (m slowMemtable) getBySecondary(int, []byte) ([]byte, []byte, error) {
	time.Sleep(m.delay)
	return nil, nil, lsmkv.NotFound
}

// Ten Bucket literals in this package omit the logger, so the slow-read
// logging must tolerate a nil one. Only a read slower than 100ms reaches that
// branch, which is why this surfaced as an intermittent SIGSEGV under CI load
// rather than as a reproducible failure.
func TestBucketSlowReadWithoutLoggerDoesNotPanic(t *testing.T) {
	const delay = 110 * time.Millisecond

	newBucket := func() Bucket {
		return Bucket{
			active: slowMemtable{
				memtable: newTestMemtableReplace(map[string][]byte{"key1": []byte("value1")}),
				delay:    delay,
			},
			disk:     &SegmentGroup{strategy: StrategyReplace},
			strategy: StrategyReplace,
			// Without this the secondary lookup returns at the
			// "no secondary index at pos" guard and never reaches the
			// logging path under test.
			secondaryIndices: 1,
		}
	}

	t.Run("get", func(t *testing.T) {
		b := newBucket()
		require.Nil(t, b.logger)

		var (
			v   []byte
			err error
		)
		require.NotPanics(t, func() { v, err = b.Get([]byte("key1")) })
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)
	})

	t.Run("get by secondary", func(t *testing.T) {
		b := newBucket()
		require.Nil(t, b.logger)

		require.NotPanics(t, func() { _, _ = b.GetBySecondary(context.Background(), 0, []byte("key1")) })
	})
}
