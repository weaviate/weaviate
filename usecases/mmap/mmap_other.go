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

//go:build !(darwin || linux)

package mmap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type MMap = mmap.MMap

func MapRegion(f *os.File, length int, prot, flags int, offset int64) (MMap, error) {
	return mmap.MapRegion(f, length, prot, flags, offset)
}
