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

package datasets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gomlx/go-huggingface/hub"
)

// hfRevision is the dataset revision this package downloads. It matches the
// default ("main") used by hub.New, and is the key under which the resolved
// commit hash is cached on disk (info/<revision>).
const hfRevision = "main"

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
	// Reuse the file from the local HuggingFace cache when present. The library's
	// DownloadFile always re-resolves the "main" revision against the API first,
	// which is rate-limited and makes CI flaky with HTTP 429 even on a warm cache.
	if cached, ok := h.cachedParquetPath(filePath); ok {
		return cached, nil
	}
	localPath, err := h.repo.DownloadFile(filePath)
	if err != nil {
		return "", err
	}
	return localPath, nil
}

// cachedParquetPath returns the path to an already-downloaded parquet file in the
// local HuggingFace cache, if present. It mirrors the on-disk layout that
// go-huggingface shares with the Python huggingface_hub library: the resolved
// commit hash for a revision lives in info/<revision>, and files are stored under
// snapshots/<commitHash>/. This lets us serve a warm cache without any network
// call. It assumes the default cache dir and revision used by this package's
// constructor.
func (h *HubDataset) cachedParquetPath(filePath string) (string, bool) {
	flatName := strings.Join(
		append([]string{string(hub.RepoTypeDataset)}, strings.Split(h.datasetID, "/")...),
		hub.RepoIdSeparator,
	)
	repoDir := filepath.Join(hub.DefaultCacheDir(), flatName)

	infoBytes, err := os.ReadFile(filepath.Join(repoDir, "info", hfRevision))
	if err != nil {
		return "", false
	}
	var info struct {
		Sha string `json:"sha"`
	}
	if err := json.Unmarshal(infoBytes, &info); err != nil || info.Sha == "" {
		return "", false
	}

	snapshotPath := filepath.Join(repoDir, "snapshots", info.Sha, filepath.FromSlash(filePath))
	if _, err := os.Stat(snapshotPath); err != nil {
		return "", false
	}
	return snapshotPath, true
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
