package segmentindex

import (
	"fmt"
	"math"

	"github.com/weaviate/weaviate/usecases/byteops"
)

func (t *DiskTree) QuantileKeys(q int) [][]byte {
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
		// TODO: handle error
		panic(fmt.Errorf("copy node key: %w", err).Error())
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
