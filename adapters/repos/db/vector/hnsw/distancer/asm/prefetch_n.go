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

//go:build amd64 || arm64

package asm

// PrefetchN issues ceil(n/64) cache-line prefetch hints, 64 bytes apart,
// counting from addr. That covers the n bytes starting at addr if addr is on
// a line boundary; for any other addr, size the call with PrefetchSpan. n must
// be > 0.
func PrefetchN(addr uintptr, n int)
