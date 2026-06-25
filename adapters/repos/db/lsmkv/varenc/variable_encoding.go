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

package varenc

type VarEncDataType uint8

const (
	SimpleUint64 VarEncDataType = iota
	SimpleUint32
	SimpleUint16
	SimpleUint8
	SimpleFloat64
	SimpleFloat32
	VarIntUint64 // Variable length encoding for uint64

	// Add new data types here
	DeltaVarIntUint64 = VarIntUint64 + 64
)

type VarEncEncoder[T any] interface {
	Init(expectedCount int)
	Encode(values []T) []byte
	Decode(data []byte) []T
	EncodeReusable(values []T, buf []byte)
	DecodeReusable(data []byte, values []T)
}

func GetVarEncEncoder64(t VarEncDataType) VarEncEncoder[uint64] {
	switch t {
	case SimpleUint64:
		return &SimpleEncoder[uint64]{}
	case VarIntUint64:
		return &VarIntEncoder{}
	case DeltaVarIntUint64:
		return &VarIntDeltaEncoder{}
	default:
		return nil
	}
}

// GetDecodeFunc returns a stateless reusable-decode function for the query read
// path. Unlike GetVarEncEncoder64 it allocates no per-call buffers and needs no
// encoder instance — DecodeReusable writes straight into the caller's slice — so
// callers avoid one heap allocation per term and the interface dispatch. Returns
// nil for codecs without a uint64 decoder (same contract as GetVarEncEncoder64).
func GetDecodeFunc(t VarEncDataType) func(data []byte, values []uint64) {
	switch t {
	case VarIntUint64:
		return func(data []byte, values []uint64) { decodeReusable(values, data, false) }
	case DeltaVarIntUint64:
		return func(data []byte, values []uint64) { decodeReusable(values, data, true) }
	case SimpleUint64:
		return decodeSimpleUint64
	default:
		return nil
	}
}
