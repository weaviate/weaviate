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
	if err := common.ParseCommonUsageConfig(config); err != nil {
		return err
	}
	if err := parseGCSConfig(config); err != nil {
		return err
	}

	// Validate required configuration
	if config.Usage.GCSBucket.Get() == "" && !config.RuntimeOverrides.Enabled {
		return fmt.Errorf("GCS bucket name not configured - set USAGE_GCS_BUCKET environment variable or enable runtime overrides with RUNTIME_OVERRIDES_ENABLED=true")
	}

	// Initialize logger
	logger := params.GetLogger().WithField("component", Name)

	// Create metrics first
	metrics := common.NewMetrics(params.GetMetricsRegisterer(), Name)

	// Create GCS storage backend with metrics
	gcsStorage, err := NewGCSStorage(ctx, logger, metrics)
	if err != nil {
		return fmt.Errorf("failed to create GCS storage: %w", err)
	}

	m.gcsStorage = gcsStorage

	// Update storage configuration (this may have empty bucket initially)
	storageConfig := m.buildGCSConfig(config)
	if _, err := m.gcsStorage.UpdateConfig(storageConfig); err != nil {
		return fmt.Errorf("failed to configure GCS storage: %w", err)
	}

	// Create base module with GCS storage
	m.BaseModule = common.NewBaseModule(Name, m.gcsStorage)

	// Initialize base module with metrics
	if err := m.InitializeCommon(ctx, config, logger, metrics); err != nil {
		return err
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

func parseGCSConfig(config *config.Config) error {
	gcsBucket := ""
	if config.Usage.GCSBucket != nil {
		gcsBucket = config.Usage.GCSBucket.Get()
	} else if v := os.Getenv("USAGE_GCS_BUCKET"); v != "" {
		gcsBucket = v
	}
	config.Usage.GCSBucket = runtime.NewDynamicValue(gcsBucket)

	gcsPrefix := ""
	if config.Usage.GCSPrefix != nil {
		gcsPrefix = config.Usage.GCSPrefix.Get()
	} else if v := os.Getenv("USAGE_GCS_PREFIX"); v != "" {
		gcsPrefix = v
	}
	config.Usage.GCSPrefix = runtime.NewDynamicValue(gcsPrefix)

	return nil
}

// verify we implement the required interfaces
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
