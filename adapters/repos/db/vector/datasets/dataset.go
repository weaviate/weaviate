//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package datasets

import (
	"fmt"

	"github.com/gomlx/go-huggingface/hub"
)

type HubDataset struct {
	repo      *hub.Repo
	datasetID string
	subset    string
}

func NewHubDataset(datasetID string, subset string) *HubDataset {
	return NewHubDatasetWithToken(datasetID, subset, "")
}

func NewHubDatasetWithToken(datasetID string, subset string, hfAuthToken string) *HubDataset {
	repo := hub.New(datasetID).
		WithType(hub.RepoTypeDataset).
		WithAuth(hfAuthToken)
	return &HubDataset{
		repo:      repo,
		datasetID: datasetID,
		subset:    subset,
	}
}

func (h *HubDataset) downloadParquetFile(name string) (string, error) {
	filePath := fmt.Sprintf("%s/%s/%s.parquet", h.subset, name, name)
	localPath, err := h.repo.DownloadFile(filePath)
	if err != nil {
		return "", err
	}
	return localPath, nil
}

const defaultRowBufferSize = 1000

func (h *HubDataset) LoadTrainData() (ids []uint64, vectors [][]float32, err error) {
	trainReader, err := h.NewDataReader(TrainSplit, 0, -1, defaultRowBufferSize)
	if err != nil {
		return nil, nil, err
	}
	defer trainReader.Close()
	ds, err := trainReader.ReadAllRows()
	if err != nil {
		return nil, nil, err
	}
	return ds.Ids, ds.Vectors, nil
}

func (h *HubDataset) LoadTestData() (neighbors [][]uint64, vectors [][]float32, err error) {
	testReader, err := h.NewDataReader(TestSplit, 0, -1, defaultRowBufferSize)
	if err != nil {
		return nil, nil, err
	}
	defer testReader.Close()
	ds, err := testReader.ReadAllRows()
	if err != nil {
		return nil, nil, err
	}
	return ds.Neighbors, ds.Vectors, nil
}
