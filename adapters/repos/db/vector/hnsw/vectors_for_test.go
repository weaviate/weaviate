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

var testLabels = []string{
	// light reds
	"pinot noir",
	"nebbiolo",
	"gamay",

	// whites
	"riesling",
	"chardonnay",
	"sauvignon blanc",

	// heavy reds
	"merlot",
	"cabernet sauvignon",
	"cabernet franc",
}

func testVectorForID(ctx context.Context, id int32) ([]float32, error) {
	return testVectors[int(id)], nil
}
