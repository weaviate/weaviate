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

package hnsw

import "context"

// roughly grouped into three clusters of three
var testVectors = [][]float32{
	{0.1, 0.9},
	{0.15, 0.8},
	{0.13, 0.65},

	{0.6, 0.1},
	{0.63, 0.2},
	{0.65, 0.08},

	{0.8, 0.8},
	{0.9, 0.75},
	{0.8, 0.7},
}

func testVectorForID(ctx context.Context, id uint64) ([]float32, error) {
	return testVectors[int(id)], nil
}

var testMultiVectors = [][][]float32{
	{
		{0.1, 0.9},
		{0.15, 0.8},
		{0.13, 0.65},
	},
	{
		{0.6, 0.1},
		{0.63, 0.2},
		{0.65, 0.08},
	},
	{
		{0.8, 0.8},
		{0.9, 0.75},
		{0.8, 0.7},
	},
	{
		{0.25, 0.45},
		{0.28, 0.42},
		{0.22, 0.48},
	},
	{
		{0.35, 0.35},
		{0.38, 0.32},
		{0.33, 0.37},
	},
	{
		{0.55, 0.55},
		{0.58, 0.52},
		{0.53, 0.57},
	},
	{
		{0.7, 0.3},
		{0.73, 0.28},
		{0.68, 0.32},
	},
	{
		{0.4, 0.85},
		{0.43, 0.82},
		{0.38, 0.87},
	},
	{
		{0.95, 0.15},
		{0.92, 0.18},
		{0.97, 0.12},
	},
}

func testMultiVectorForID(ctx context.Context, id uint64) ([][]float32, error) {
	return testMultiVectors[int(id)], nil
}
