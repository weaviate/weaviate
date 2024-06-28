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

package ent

type VectorizationResult struct {
	TextVectors    [][]float32
	ImageVectors   [][]float32
	AudioVectors   [][]float32
	VideoVectors   [][]float32
	IMUVectors     [][]float32
	ThermalVectors [][]float32
	DepthVectors   [][]float32
}
