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

package usagegcs

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	common "github.com/weaviate/weaviate/modules/usagecommon"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	Name = "usage-gcs"
)

// module is the GCS usage module using the common base
type module struct {
	*common.BaseModule
	gcsStorage *GCSStorage
}

func New() *module {
	return &module{}
}

func (m *module) SetUsageService(usageService any) {
	m.BaseModule.SetUsageService(usageService)
}

func (m *module) Name() string {
	return Name
}

func (m *module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Usage
}

func (m *module) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	// Parse usage configuration from environment
	config := params.GetConfig()
	if err := parseUsageConfig(config); err != nil {
		return err
	}

	// Validate required configuration
	if config.Usage.GCSBucket == nil || config.Usage.GCSBucket.Get() == "" {
		return fmt.Errorf("GCP bucket name not configured")
	}

	// Initialize logger
	logger := params.GetLogger().WithField("component", Name)

	metrics := common.NewMetrics(params.GetMetricsRegisterer(), Name)

	// Create GCS storage backend with metrics
	gcsStorage, err := NewGCSStorage(ctx, logger, metrics)
	if err != nil {
		return fmt.Errorf("failed to create GCS storage: %w", err)
	}
	m.gcsStorage = gcsStorage

	// Set nodeID directly during initialization
	m.gcsStorage.nodeID = config.Cluster.Hostname

	// Create base module with GCS storage
	m.BaseModule = common.NewBaseModule(Name, gcsStorage)

	// Initialize base module with metrics
	if err := m.BaseModule.InitializeCommon(ctx, config, logger, metrics); err != nil {
		return err
	}

	// Update GCS storage with initial configuration
	storageConfig := m.buildGCSConfig(config)
	if _, err := m.gcsStorage.UpdateConfig(storageConfig); err != nil {
		return fmt.Errorf("failed to configure GCS storage: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"node_id":             config.Cluster.Hostname,
		"collection_interval": config.Usage.ScrapeInterval.Get(),
		"gcs_bucket":          config.Usage.GCSBucket.Get(),
		"gcs_prefix":          config.Usage.GCSPrefix.Get(),
	}).Info("initializing usage-gcs module with configuration")

	return nil
}

func (m *module) buildGCSConfig(config *config.Config) common.StorageConfig {
	storageConfig := common.StorageConfig{
		NodeID: config.Cluster.Hostname,
	}

	if config.Usage.GCSBucket != nil {
		storageConfig.Bucket = config.Usage.GCSBucket.Get()
	}
	if config.Usage.GCSPrefix != nil {
		storageConfig.Prefix = config.Usage.GCSPrefix.Get()
	}
	if config.Usage.PolicyVersion != nil {
		storageConfig.Version = config.Usage.PolicyVersion.Get()
	}

	return storageConfig
}

func parseUsageConfig(config *config.Config) error {
	if v := os.Getenv("USAGE_GCS_BUCKET"); v != "" {
		config.Usage.GCSBucket = runtime.NewDynamicValue(v)
	}
	if v := os.Getenv("USAGE_GCS_PREFIX"); v != "" {
		config.Usage.GCSPrefix = runtime.NewDynamicValue(v)
	}
	if v := os.Getenv("USAGE_SCRAPE_INTERVAL"); v != "" {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", "USAGE_SCRAPE_INTERVAL", err)
		}
		config.Usage.ScrapeInterval = runtime.NewDynamicValue(duration)
	}
	if v := os.Getenv("USAGE_POLICY_VERSION"); v != "" {
		config.Usage.PolicyVersion = runtime.NewDynamicValue(v)
	}
	return nil
}

// verify we implement the required interfaces
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
