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

package usages3

import (
	"context"
	"fmt"
	"os"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
)

const (
	Name = "usage-s3"
)

// module is the S3 usage module using the common base
type module struct {
	*common.BaseModule
	s3Storage *S3Storage
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
	if err := parseS3Config(config); err != nil {
		return err
	}

	// Validate required configuration
	if config.Usage.S3Bucket.Get() == "" && !config.RuntimeOverrides.Enabled {
		return fmt.Errorf("S3 bucket name not configured - set USAGE_S3_BUCKET environment variable or enable runtime overrides with RUNTIME_OVERRIDES_ENABLED=true")
	}

	// Initialize logger
	logger := params.GetLogger().WithField("component", Name)

	// Create metrics first
	metrics := common.NewMetrics(params.GetMetricsRegisterer(), Name)

	// Create S3 storage backend with metrics
	s3Storage, err := NewS3Storage(ctx, logger, metrics)
	if err != nil {
		return fmt.Errorf("failed to create S3 storage: %w", err)
	}

	m.s3Storage = s3Storage

	// Update storage configuration (this may have empty bucket initially)
	storageConfig := m.buildS3Config(config)
	if _, err := m.s3Storage.UpdateConfig(storageConfig); err != nil {
		return fmt.Errorf("failed to configure S3 storage: %w", err)
	}

	// Create base module with S3 storage
	m.BaseModule = common.NewBaseModule(Name, m.s3Storage)

	// Initialize base module with metrics
	if err := m.InitializeCommon(ctx, config, logger, metrics); err != nil {
		return err
	}

	// Build log fields, omitting empty values for cleaner output
	logFields := map[string]interface{}{
		"node_id":             config.Cluster.Hostname,
		"collection_interval": config.Usage.ScrapeInterval.Get().String(),
	}

	if bucket := config.Usage.S3Bucket.Get(); bucket != "" {
		logFields["s3_bucket"] = bucket
	} else if config.RuntimeOverrides.Enabled {
		logFields["s3_bucket"] = "[pending runtime overrides]"
	}

	if prefix := config.Usage.S3Prefix.Get(); prefix != "" {
		logFields["s3_prefix"] = prefix
	} else if config.RuntimeOverrides.Enabled {
		logFields["s3_prefix"] = "[pending runtime overrides]"
	}

	logger.WithFields(logFields).Info("initializing usage-s3 module with configuration")

	return nil
}

func (m *module) buildS3Config(config *config.Config) common.StorageConfig {
	storageConfig := common.StorageConfig{
		NodeID: config.Cluster.Hostname,
	}

	if config.Usage.S3Bucket != nil {
		storageConfig.Bucket = config.Usage.S3Bucket.Get()
	}
	if config.Usage.S3Prefix != nil {
		storageConfig.Prefix = config.Usage.S3Prefix.Get()
	}
	if config.Usage.PolicyVersion != nil {
		storageConfig.Version = config.Usage.PolicyVersion.Get()
	}

	return storageConfig
}

func parseS3Config(config *config.Config) error {
	s3Bucket := ""
	if v := os.Getenv("USAGE_S3_BUCKET"); v != "" {
		s3Bucket = v
	} else if config.Usage.S3Bucket != nil {
		s3Bucket = config.Usage.S3Bucket.Get()
	}
	config.Usage.S3Bucket = runtime.NewDynamicValue(s3Bucket)

	s3Prefix := ""
	if v := os.Getenv("USAGE_S3_PREFIX"); v != "" {
		s3Prefix = v
	} else if config.Usage.S3Prefix != nil {
		s3Prefix = config.Usage.S3Prefix.Get()
	}
	config.Usage.S3Prefix = runtime.NewDynamicValue(s3Prefix)

	return nil
}

// verify we implement the required interfaces
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
