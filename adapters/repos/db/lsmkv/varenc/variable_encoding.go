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

package varenc

type VarEncDataType uint8

const (
	SimpleUint64 VarEncDataType = iota
	SimpleUint32
	SimpleUint16
	SimpleUint8
	SimpleFloat64
	SimpleFloat32
	VarIntUint64      = 0x40
	VarIntUint64Delta = 0x41
)
