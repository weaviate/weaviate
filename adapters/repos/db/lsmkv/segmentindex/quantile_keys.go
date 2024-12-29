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

package segmentindex

import (
	"math"

	"github.com/weaviate/weaviate/usecases/byteops"
)

// QuantileKeys returns a list of keys that roughly represent the quantiles of
// the tree. This can be very useful to bootstrap parallel cursors over the
// segment that are more or less evenly distributed.
//
// This method uses the natural shape of the tree to determine the
// distribution of the keys. This is a performance-accuracy trade-off. It does
// not guarantee perfect distribution, but it is fairly cheap to obtain as most
// runs will only need to go a few levels deep – even on massive trees.
//
// The number of keys returned is not guaranteed to be exactly q, in most cases
// returns more keys. This is because in a real-life application you would
// likely aggregate across multiple segments. Similarly keys are not returned
// in any specific order, as the assumption is that post-processing will be
// done when keys are aggregated across multiple segments.
//
// The two guarantees you get are:
//
//  1. If there are at least q keys in the tree, you will get at least q keys,
//     most likely more
//  2. If there are less than q keys in the tree, you will get all keys.
func (t *DiskTree) QuantileKeys(q int) [][]byte {
	if q <= 0 {
		return nil
	}

	// we will overfetch a bit because we will have q keys at level n, but in
	// addition we can use all keys discovered on the way to get to level n. This
	// will help us to get a more even distribution of keys – especially when
	// multiple trees need to merged down the line (e.g. because there are many
	// segements).
	depth := int(math.Ceil(math.Log2(float64(q))))
	bfs := parallelBFS{
		dt:             t,
		maxDepth:       depth,
		keysDiscovered: make([][]byte, 0, depth),
	}

	return bfs.run()
}

type parallelBFS struct {
	dt             *DiskTree
	maxDepth       int
	keysDiscovered [][]byte
}

func (bfs *parallelBFS) run() [][]byte {
	bfs.parse(0, 0)

	return bfs.keysDiscovered
}

func (bfs *parallelBFS) parse(offset uint64, level int) {
	if offset+4 > uint64(len(bfs.dt.data)) || offset+4 < 4 {
		// exit condition
		return
	}
	rw := byteops.NewReadWriter(bfs.dt.data)
	rw.Position = offset
	keyLen := rw.ReadUint32()
	nodeKeyBuffer := make([]byte, int(keyLen))
	_, err := rw.CopyBytesFromBuffer(uint64(keyLen), nodeKeyBuffer)
	if err != nil {
		// no special handling other than skipping this node. If the key could not
		// be read correctly, we have much bigger problems worrying about quantile
		// keys for cursor efficiency. This error is handled during normal .Get()
		// operations. It is not worth changing the signature of quantile keys just
		// to return this one error. We could also consider explicitly panic'ing
		// here, so this error does not get lost.
		return
	}

	bfs.keysDiscovered = append(bfs.keysDiscovered, nodeKeyBuffer)

	if level+1 > bfs.maxDepth {
		return
	}

	rw.MoveBufferPositionForward(2 * 8) // jump over start+end position
	leftChildPos := rw.ReadUint64()     // left child
	rightChildPos := rw.ReadUint64()    // left child

	bfs.parse(leftChildPos, level+1)
	bfs.parse(rightChildPos, level+1)
}
