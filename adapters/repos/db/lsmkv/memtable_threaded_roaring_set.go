//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"math/rand"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
)

func (m *MemtableThreaded) roaringOperation(data ThreadedBitmapRequest) ThreadedBitmapResponse {
	if data.key == nil {
		return ThreadedBitmapResponse{error: errors.Errorf("key is nil")}
	}
	key := data.key
	workerID := 0
	if m.workerAssignment == "single-channel" {
		workerID = 0
	} else if m.workerAssignment == "random" {
		workerID = rand.Intn(m.numWorkers)
	} else if m.workerAssignment == "hash" {
		workerID = threadHashKey(key, m.numWorkers)
	} else {
		panic("invalid worker assignment")
	}
	responseCh := make(chan ThreadedBitmapResponse)
	data.response = responseCh
	m.requestsChannels[workerID] <- data
	return <-responseCh
}

func (m *MemtableThreaded) roaringSetAddOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddOne(key, value)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddOne,
			key:       key,
			value:     value,
		})
		return output.error
	}

}

func (m *MemtableThreaded) roaringSetAddList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddList(key, values)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddList,
			key:       key,
			values:    values,
		})
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddBitmap(key, bm)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddBitmap,
			key:       key,
			bm:        bm,
		})
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveOne(key []byte, value uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveOne(key, value)
	}
	return errors.Errorf("baseline is nil")
}

func (m *MemtableThreaded) roaringSetRemoveList(key []byte, values []uint64) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveList(key, values)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetRemoveList,
			key:       key,
			values:    values,
		})
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetRemoveBitmap(key []byte, bm *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetRemoveBitmap(key, bm)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetRemoveBitmap,
			key:       key,
			bm:        bm,
		})
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetAddRemoveBitmaps(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddRemoveBitmaps(key, additions, deletions)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetAddRemoveBitmaps,
			key:       key,
			additions: additions,
			deletions: deletions,
		})
		return output.error
	}
}

func (m *MemtableThreaded) roaringSetGet(key []byte) (roaringset.BitmapLayer, error) {
	if m.baseline != nil {
		return m.baseline.roaringSetGet(key)
	} else {
		output := m.roaringOperation(ThreadedBitmapRequest{
			operation: ThreadedRoaringSetGet,
			key:       key,
		})
		return output.bitmap, output.error
	}
}

func (m *MemtableThreaded) roaringSetAdjustMeta(entriesChanged int) {
	if m.baseline != nil {
		m.baseline.roaringSetAdjustMeta(entriesChanged)
	} else {
		for _, channel := range m.responseChannels {
			channel <- nil
		}
	}
}

func (m *MemtableThreaded) roaringSetAddCommitLog(key []byte, additions *sroar.Bitmap, deletions *sroar.Bitmap) error {
	if m.baseline != nil {
		return m.baseline.roaringSetAddCommitLog(key, additions, deletions)
	} else {
		return nil
	}
}
