package packedconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var exampleConns = []uint64{
	4477, 83, 6777, 13118, 12903, 12873, 14397, 15034, 15127, 15162, 15219, 15599, 17627,
	18624, 18844, 19359, 22981, 23099, 36188, 37400, 39724, 39810, 47254, 58047, 59647, 61746,
	64635, 66528, 70470, 73936, 86283, 86697, 120033, 129098, 131345, 137609, 140937, 186468,
	191226, 199803, 206818, 223456, 271063, 278598, 288539, 395876, 396785, 452103, 487237,
	506431, 507230, 554813, 572566, 595572, 660562, 694477, 728865, 730031, 746368, 809331,
	949338,
}

func TestConnections(t *testing.T) {
	// c.ReplaceLayer(0, exampleConns)

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
	c.ReplaceLayer(0, exampleConns)
	assert.ElementsMatch(t, exampleConns, c.GetLayer(0))
	assert.Len(t, c.GetLayer(1), 0)
	assert.Len(t, c.GetLayer(2), 0)
}
