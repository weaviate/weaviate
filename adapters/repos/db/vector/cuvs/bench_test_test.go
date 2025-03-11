//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//  CONTACT: hello@weaviate.io
//

//go:build cuvs

package cuvs_index

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	// "fmt"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/hdf5"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	cuvsEnt "github.com/weaviate/weaviate/entities/vectorindex/cuvs"
)

// BenchmarkDataset runs a single benchmark configuration (without pareto sweeps)
// using the default cuvs settings.
func TestDataset(b *testing.T) {
	// Exclude setup time.

	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(
		filepath.Join(b.TempDir(), "store"),
		b.TempDir(),
		logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
	)
	if err != nil {
		b.Fatal("failed to create store")
	}
	defer store.Shutdown(context.Background())

	index, err := New(Config{
		ID:             "a",
		TargetVector:   "vector",
		Logger:         logger,
		RootPath:       b.TempDir(),
		CuvsPoolMemory: 90,
	}, cuvsEnt.NewDefaultUserConfig(), store)
	if err != nil {
		b.Fatal(err)
	}

	index.PostStartup()

	file, err := hdf5.OpenFile("/home/ajit/datasets/dbpedia-openai-1000k-angular.hdf5", hdf5.F_ACC_RDONLY)
	if err != nil {
		b.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	trainDataset, err := file.OpenDataset("train")
	if err != nil {
		b.Fatalf("Error opening train dataset: %v", err)
	}
	testDataset, err := file.OpenDataset("test")
	if err != nil {
		b.Fatalf("Error opening test dataset: %v", err)
	}
	neighborsDataset, err := file.OpenDataset("neighbors")
	if err != nil {
		b.Fatalf("Error opening neighbors dataset: %v", err)
	}

	// Run the benchmark b.N times.

	BuildTime, BuildQPS := LoadVectors(index, trainDataset)
	QueryRecall, QueryTime, QueryQPS := QueryVectors(index, testDataset, neighborsDataset)

	fmt.Println(BuildQPS, BuildTime, QueryQPS, QueryTime, QueryRecall)
}
