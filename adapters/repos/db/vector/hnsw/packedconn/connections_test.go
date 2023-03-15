package packedconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	// TODO: grow dynamically
	c := Connections{data: make([]byte, 100000)}
	c.ReplaceLayer(0, exampleConns)

	res := c.GetLayer(0)
	assert.ElementsMatch(t, exampleConns, res)
}
