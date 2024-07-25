package segmentindex

import (
	"fmt"
	"sync"

	"github.com/weaviate/weaviate/usecases/byteops"
)

func (t *DiskTree) QuantileKeys(q int) [][]byte {
	bfs := parallelBFS{
		dt:             t,
		maxDepth:       8,
		keysDiscovered: make(chan []byte),
	}

	return bfs.run()
}

type parallelBFS struct {
	dt             *DiskTree
	maxDepth       int
	keysDiscovered chan []byte
	wg             sync.WaitGroup
}

func (bfs *parallelBFS) run() [][]byte {
	bfs.wg.Add(1)
	go bfs.parse(0, 0)

	var keys [][]byte
	go func() {
		for key := range bfs.keysDiscovered {
			keys = append(keys, key)
			bfs.wg.Done()
		}
	}()
	bfs.wg.Wait()

	close(bfs.keysDiscovered)

	return keys
}

func (bfs *parallelBFS) parse(offset uint64, level int) {
	// TODO: check exit condition (when have we reached the end of the tree)
	fmt.Printf("offset=%d, max=%d level=%d\n", offset, len(bfs.dt.data), level)
	rw := byteops.NewReadWriter(bfs.dt.data)
	rw.Position = offset
	keyLen := rw.ReadUint32()
	nodeKeyBuffer := make([]byte, int(keyLen))
	_, err := rw.CopyBytesFromBuffer(uint64(keyLen), nodeKeyBuffer)
	if err != nil {
		// TODO: handle error
		panic(fmt.Errorf("copy node key: %w", err).Error())
	}
	bfs.keysDiscovered <- nodeKeyBuffer

	if level+1 >= bfs.maxDepth {
		return
	}

	rw.MoveBufferPositionForward(2 * 8) // jump over start+end position
	leftChildPos := rw.ReadUint64()     // left child
	rightChildPos := rw.ReadUint64()    // left child

	bfs.wg.Add(2)
	bfs.parse(leftChildPos, level+1)
	bfs.parse(rightChildPos, level+1)
}
