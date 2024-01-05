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

// Key is a helper struct that can be used to build the key nodes for the
// segment index. It contains the primary key and an arbitrary number of
// secondary keys, as well as valueStart and valueEnd indicator. Those are used
// to find the correct payload for each key.
type Key struct {
	Key           []byte
	SecondaryKeys [][]byte
	ValueStart    int
	ValueEnd      int
}
