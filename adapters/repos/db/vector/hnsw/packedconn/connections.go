package packedconn

import (
	"encoding/binary"
	"fmt"
	"sort"
)

type Connections struct {
	data []byte
}

func (c *Connections) ReplaceLayer(layer int, conns []uint64) {
	if layer != 0 {
		panic("no support for multiple layers yet")
	}

	sort.Slice(conns, func(a, b int) bool { return conns[a] < conns[b] })
	last := uint64(0)
	offset := 0
	for _, raw := range conns {
		delta := raw - last
		last = raw
		offset += binary.PutUvarint(c.data[offset:], delta)
	}

	// TODO: grow and size dynamically
	c.data = c.data[:offset]

	orig := len(conns) * 8
	fmt.Printf("wrote %d bytes, raw would be %d (%f%%)\n", offset, orig, float64(offset)/float64(orig)*100)
}

func (c Connections) GetLayer(layer int) []uint64 {
	if layer != 0 {
		panic("no support for multiple layers yet")
	}

	var conns []uint64

	offset := 0
	last := uint64(0)
	for offset < len(c.data) {
		val, n := binary.Uvarint(c.data[offset:])
		offset += n

		// TODO: allocate exact size, don't rely on dynamic growing
		conns = append(conns, last+val)
		last += val
	}

	return conns
}
