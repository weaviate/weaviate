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

package lsmkv

import (
	"context"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
)

func TestMemtableRoaringSetRange(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	logger, _ := test.NewNullLogger()
	memPath := func() string {
		return path.Join(t.TempDir(), "memtable")
	}

	t.Run("concurrent writes and search", func(t *testing.T) {
		cl, err := newCommitLogger(memPath(), StrategyRoaringSetRange, 0)
		require.NoError(t, err)
		m, err := newMemtable(memPath(), StrategyRoaringSetRange, 0, cl, nil, logger, false, nil)
		require.Nil(t, err)

		addKeyVals := func(k uint64) error {
			return m.roaringSetRangeAdd(k, k+1000, k+2000, k+3000)
		}
		removeKeyVals := func(k uint64) error {
			return m.roaringSetRangeRemove(k, k+1000, k+2000, k+3000)
		}
		assertRead_LTE4_GT7 := func(t *testing.T, reader roaringsetrange.InnerReader) {
			expAddLTE4 := []uint64{
				1000, 2000, 3000,
				1002, 2002, 3002,
				1004, 2004, 3004,
			}
			expAddGT7 := []uint64{
				1008, 2008, 3008,
			}
			expDel := []uint64{
				1000, 2000, 3000,
				1001, 2001, 3001,
				1002, 2002, 3002,
				1003, 2003, 3003,
				1004, 2004, 3004,
				1005, 2005, 3005,
				1006, 2006, 3006,
				1007, 2007, 3007,
				1008, 2008, 3008,
				1009, 2009, 3009,
			}

			layerLTE4, release, err := reader.Read(context.Background(), 4, filters.OperatorLessThanEqual)
			require.NoError(t, err)
			defer release()

			layerGT7, release, err := reader.Read(context.Background(), 7, filters.OperatorGreaterThan)
			require.NoError(t, err)
			defer release()

			assert.ElementsMatch(t, expAddLTE4, layerLTE4.Additions.ToArray())
			assert.ElementsMatch(t, expDel, layerLTE4.Deletions.ToArray())
			assert.ElementsMatch(t, expAddGT7, layerGT7.Additions.ToArray())
			assert.ElementsMatch(t, expDel, layerGT7.Deletions.ToArray())
		}

		// populate with initial data
		for i := uint64(0); i < 10; i = i + 2 {
			assert.NoError(t, addKeyVals(i))
		}
		for i := uint64(1); i < 10; i = i + 2 {
			assert.NoError(t, removeKeyVals(i))
		}

		// create reader
		reader := m.newRoaringSetRangeReader()

		// assert data
		assertRead_LTE4_GT7(t, reader)

		// concurrently mutate memtable
		chStart := make(chan struct{})
		chFinish := make(chan struct{})
		go func() {
			chStart <- struct{}{}
			for {
				select {
				case <-chFinish:
					return
				default:
					addKeyVals(uint64(rnd.Int31n(1000)))
					removeKeyVals(uint64(rnd.Int31n(1000)))
				}
			}
		}()

		// assert search results do not contain muted data
		<-chStart
		for i := 0; i < 256; i++ {
			assertRead_LTE4_GT7(t, reader)
		}
		chFinish <- struct{}{}
	})
}
