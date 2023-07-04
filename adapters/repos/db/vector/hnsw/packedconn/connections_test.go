package packedconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	connsSlice1 = []uint64{
		4477, 83, 6777, 13118, 12903, 12873, 14397, 15034, 15127, 15162, 15219, 15599, 17627,
		18624, 18844, 19359, 22981, 23099, 36188, 37400, 39724, 39810, 47254, 58047, 59647, 61746,
		64635, 66528, 70470, 73936, 86283, 86697, 120033, 129098, 131345, 137609, 140937, 186468,
		191226, 199803, 206818, 223456, 271063, 278598, 288539, 395876, 396785, 452103, 487237,
		506431, 507230, 554813, 572566, 595572, 660562, 694477, 728865, 730031, 746368, 809331,
		949338,
	}
	connsSlice2 = []uint64{
		10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
	}
	connsSlice3 = []uint64{
		9999, 10000, 10001,
	}
)

func TestConnections_ReplaceLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	// Initially all layers should have length==0 and return no results
	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.GetLayer(0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.GetLayer(2), 0)

	// replace layer 0, it should return the correct results, all others should
	// still be empty
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Len(t, c.GetLayer(2), 0)

	// replace layer 1+2, other layers should be unaffected
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))

	// replace a layer with a smaller list to trigger a shrinking operation
	c.ReplaceLayer(2, []uint64{768})
	assert.ElementsMatch(t, []uint64{768}, c.GetLayer(2))
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))

	// replace the other layers with smaller lists
	c.ReplaceLayer(0, connsSlice1[:5])
	c.ReplaceLayer(1, connsSlice2[:5])
	assert.ElementsMatch(t, connsSlice1[:5], c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2[:5], c.GetLayer(1))

	// finally grow all layers back to their original sizes again, to verify what
	// previous shrinking does not hinder future growing
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.GetLayer(0))
	assert.ElementsMatch(t, connsSlice2, c.GetLayer(1))
	assert.ElementsMatch(t, connsSlice3, c.GetLayer(2))
}

func TestConnections_CopyLayers(t *testing.T) {
	c, err := NewWithMaxLayer(2)
	require.Nil(t, err)

	conns := make([]uint64, 0, 100)

	// Initially all layers should have length==0 and return no results
	assert.Equal(t, 0, c.LenAtLayer(0))
	assert.Len(t, c.CopyLayer(conns, 0), 0)
	assert.Equal(t, 0, c.LenAtLayer(1))
	assert.Len(t, c.CopyLayer(conns, 1), 0)
	assert.Equal(t, 0, c.LenAtLayer(2))
	assert.Len(t, c.CopyLayer(conns, 2), 0)

	// replace layer 0, it should return the correct results, all others should
	// still be empty
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.Len(t, c.CopyLayer(conns, 1), 0)
	assert.Len(t, c.CopyLayer(conns, 2), 0)

	// replace layer 1+2, other layers should be unaffected
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))
	assert.ElementsMatch(t, connsSlice3, c.CopyLayer(conns, 2))

	// replace a layer with a smaller list to trigger a shrinking operation
	c.ReplaceLayer(2, []uint64{768})
	assert.ElementsMatch(t, []uint64{768}, c.CopyLayer(conns, 2))
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))

	// replace the other layers with smaller lists
	c.ReplaceLayer(0, connsSlice1[:5])
	c.ReplaceLayer(1, connsSlice2[:5])
	assert.ElementsMatch(t, connsSlice1[:5], c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2[:5], c.CopyLayer(conns, 1))

	// finally grow all layers back to their original sizes again, to verify what
	// previous shrinking does not hinder future growing
	c.ReplaceLayer(1, connsSlice2)
	c.ReplaceLayer(2, connsSlice3)
	c.ReplaceLayer(0, connsSlice1)
	assert.ElementsMatch(t, connsSlice1, c.CopyLayer(conns, 0))
	assert.ElementsMatch(t, connsSlice2, c.CopyLayer(conns, 1))
	assert.ElementsMatch(t, connsSlice3, c.CopyLayer(conns, 2))
}
