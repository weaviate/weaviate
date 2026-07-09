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

//go:build !linux

package lsmkv

import "os"

// fadviseSequential is a no-op on non-Linux platforms.
func fadviseSequential(_ *os.File) error {
	return nil
}

// fadviseRandom is a no-op on non-Linux platforms.
func fadviseRandom(_ *os.File) error {
	return nil
}

// madviseRandom is a no-op on non-Linux platforms.
func madviseRandom(_ []byte) error {
	return nil
}

// fadviseDontNeed is a no-op on non-Linux platforms.
func fadviseDontNeed(_ *os.File, _ int64) error {
	return nil
}
