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

package testinghelpers

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// SegmentSideLengths reads each roaring-set node's stored additions and
// deletions lengths, keyed by the node's key. It reads the length indicators
// rather than Additions()/Deletions(), so an empty side shows as the zero the
// format stores instead of the nil an accessor returns.
//
// It walks the body rather than the index, so it also says the nodes tile the
// span it was given exactly — pass the bytes between the header and IndexStart
// and a node that over- or under-runs that span fails here.
func SegmentSideLengths(t *testing.T, nodes []byte) map[string][2]uint64 {
	t.Helper()

	out := map[string][2]uint64{}
	at := 0
	for at < len(nodes) {
		nodeLen := binary.LittleEndian.Uint64(nodes[at : at+8])
		require.NotZero(t, nodeLen, "node at %d reports a zero length", at)
		aLen := binary.LittleEndian.Uint64(nodes[at+8 : at+16])
		dLen := binary.LittleEndian.Uint64(nodes[at+16+int(aLen) : at+24+int(aLen)])
		keyOff := at + 24 + int(aLen) + int(dLen)
		keyLen := binary.LittleEndian.Uint32(nodes[keyOff : keyOff+4])
		out[string(nodes[keyOff+4:keyOff+4+int(keyLen)])] = [2]uint64{aLen, dLen}
		at += int(nodeLen)
	}
	require.Equal(t, len(nodes), at,
		"the nodes do not tile the span between the header and IndexStart")
	return out
}
