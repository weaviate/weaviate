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

package hnsw

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// AdaptiveEFStatus represents the current adaptive EF calibration status for an index.
type AdaptiveEFStatus struct {
	Status            string
	Enabled           bool
	TargetRecall      float32
	WeightedAverageEF int64
}

const (
	metadataPrefix = "meta"
	metadataBucket = "hnsw_metadata"
	adaptiveEFKey  = "adaptive_ef_config"
)

func (h *hnsw) getMetadataFile() string {
	tv := h.getTargetVector()
	if tv != "" {
		cleanTarget := filepath.Clean(tv)
		cleanTarget = filepath.Base(cleanTarget)
		return metadataPrefix + "_" + cleanTarget + ".db"
	}
	return metadataPrefix + ".db"
}

// openMetadata opens (or creates) the bolt DB metadata file for this index.
func (h *hnsw) openMetadata() (*bolt.DB, error) {
	path := filepath.Join(h.rootPath, h.getMetadataFile())
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "open metadata %q", path)
	}
	return db, nil
}

// StoreAdaptiveEFConfig persists the adaptive EF config to the metadata bolt DB.
func (h *hnsw) StoreAdaptiveEFConfig(cfg *adaptiveEfConfig) error {
	db, err := h.openMetadata()
	if err != nil {
		return err
	}
	defer db.Close()

	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "marshal adaptive ef config")
	}

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(metadataBucket))
		if err != nil {
			return errors.Wrap(err, "create metadata bucket")
		}
		return b.Put([]byte(adaptiveEFKey), data)
	})
}

// LoadAdaptiveEFConfig loads the adaptive EF config from the metadata bolt DB.
// Returns nil, nil if no config has been stored yet.
func (h *hnsw) LoadAdaptiveEFConfig() (*adaptiveEfConfig, error) {
	path := filepath.Join(h.rootPath, h.getMetadataFile())
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}

	db, err := h.openMetadata()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var cfg *adaptiveEfConfig
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metadataBucket))
		if b == nil {
			return nil
		}
		data := b.Get([]byte(adaptiveEFKey))
		if data == nil {
			return nil
		}
		cfg = &adaptiveEfConfig{}
		if err := json.Unmarshal(data, cfg); err != nil {
			return errors.Wrap(err, "unmarshal adaptive ef config")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if cfg != nil {
		cfg.buildSketch()
	}
	return cfg, nil
}

// removeMetadataFile removes the metadata bolt DB file during drop.
func (h *hnsw) removeMetadataFile(keepFiles bool) {
	if keepFiles {
		return
	}
	path := filepath.Join(h.rootPath, h.getMetadataFile())
	os.Remove(path)
}

// AdaptiveEFStatusProvider is the interface for retrieving adaptive EF status
// from a vector index.
type AdaptiveEFStatusProvider interface {
	GetAdaptiveEFStatus() AdaptiveEFStatus
}

// GetAdaptiveEFStatus returns the current adaptive EF status for this index.
func (h *hnsw) GetAdaptiveEFStatus() AdaptiveEFStatus {
	if h.adaptiveEfCalibrating.Load() {
		return AdaptiveEFStatus{
			Status: "calibrating",
		}
	}

	cfg := h.adaptiveEf.Load()
	if cfg == nil {
		return AdaptiveEFStatus{
			Status:  "not_configured",
			Enabled: false,
		}
	}

	return AdaptiveEFStatus{
		Status:            "ready",
		Enabled:           true,
		TargetRecall:      cfg.TargetRecall,
		WeightedAverageEF: int64(cfg.WeightedAverageEf),
	}
}

// AsAdaptiveEFStatusProvider attempts to extract the underlying *hnsw from a VectorIndex.
// It also handles wrappers (e.g. dynamic index) that implement underlyingVectorIndex.
func AsAdaptiveEFStatusProvider(vi any) (AdaptiveEFStatusProvider, bool) {
	if h, ok := vi.(*hnsw); ok {
		return h, true
	}
	if wrapper, ok := vi.(underlyingVectorIndex); ok {
		return AsAdaptiveEFStatusProvider(wrapper.UnderlyingVectorIndex())
	}
	return nil, false
}
