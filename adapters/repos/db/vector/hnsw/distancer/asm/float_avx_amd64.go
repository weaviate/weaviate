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

//go:build amd64

package asm

//go:noescape
func dotAVX2(x, y []float32) float32

//go:noescape
func dotAVX512(x, y []float32) float32

//go:noescape
func l2AVX2(x, y []float32) float32

//go:noescape
func l2AVX512(x, y []float32) float32
