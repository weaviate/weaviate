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
	common "github.com/weaviate/weaviate/modules/usagecommon"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
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
	if config.Usage.S3Bucket == nil || config.Usage.S3Bucket.Get() == "" {
		return fmt.Errorf("S3 bucket name not configured")
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

	// Set nodeID directly during initialization
	m.s3Storage.NodeID = config.Cluster.Hostname

	// Create base module with S3 storage
	m.BaseModule = common.NewBaseModule(Name, s3Storage)

	// Initialize base module with metrics
	if err := m.InitializeCommon(ctx, config, logger, metrics); err != nil {
		return err
	}

	// Update S3 storage with initial configuration
	storageConfig := m.buildS3Config(config)
	if _, err := m.s3Storage.UpdateConfig(storageConfig); err != nil {
		return fmt.Errorf("failed to configure S3 storage: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"node_id":             config.Cluster.Hostname,
		"collection_interval": config.Usage.ScrapeInterval.Get(),
		"s3_bucket":           config.Usage.S3Bucket.Get(),
		"s3_prefix":           config.Usage.S3Prefix.Get(),
	}).Info("initializing usage-s3 module with configuration")

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
	if v := os.Getenv("USAGE_S3_BUCKET"); v != "" {
		config.Usage.S3Bucket = runtime.NewDynamicValue(v)
	}
	if v := os.Getenv("USAGE_S3_PREFIX"); v != "" {
		config.Usage.S3Prefix = runtime.NewDynamicValue(v)
	}
	return nil
}

// verify we implement the required interfaces
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
