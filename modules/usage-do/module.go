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

package usagedo

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
	Name = "usage-do"
)

// module is the DigitalOcean Spaces usage module using the common base
type module struct {
	*common.BaseModule
	doStorage *DOStorage
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
	if err := parseDOConfig(config); err != nil {
		return err
	}

	// Validate required configuration
	if config.Usage.DOBucket.Get() == "" && !config.RuntimeOverrides.Enabled {
		return fmt.Errorf("DigitalOcean Spaces bucket name not configured - set USAGE_DO_BUCKET environment variable or enable runtime overrides with RUNTIME_OVERRIDES_ENABLED=true")
	}

	if config.Usage.DORegion.Get() == "" {
		return fmt.Errorf("DigitalOcean Spaces region not configured - set USAGE_DO_REGION environment variable (e.g. nyc3, sfo3, ams3)")
	}

	// Initialize logger
	logger := params.GetLogger().WithField("component", Name)

	// Create metrics first
	metrics := common.NewMetrics(params.GetMetricsRegisterer(), Name)

	// Build endpoint from region if not explicitly set
	endpoint := config.Usage.DOEndpoint.Get()
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://%s.digitaloceanspaces.com", config.Usage.DORegion.Get())
		config.Usage.DOEndpoint = runtime.NewDynamicValue(endpoint)
	}

	// Create DO storage backend with metrics
	doStorage, err := NewDOStorage(ctx, logger, metrics, config.Usage.DORegion.Get(), endpoint)
	if err != nil {
		return fmt.Errorf("failed to create DigitalOcean Spaces storage: %w", err)
	}

	m.doStorage = doStorage

	// Update storage configuration (this may have empty bucket initially)
	storageConfig := m.buildDOConfig(config)
	if _, err := m.doStorage.UpdateConfig(storageConfig); err != nil {
		return fmt.Errorf("failed to configure DigitalOcean Spaces storage: %w", err)
	}

	// Create base module with DO storage
	m.BaseModule = common.NewBaseModule(Name, m.doStorage)

	// Initialize base module with metrics
	if err := m.InitializeCommon(ctx, config, logger, metrics); err != nil {
		return err
	}

	// Build log fields, omitting empty values for cleaner output
	logFields := map[string]interface{}{
		"node_id":             config.Cluster.Hostname,
		"collection_interval": config.Usage.ScrapeInterval.Get().String(),
		"do_region":           config.Usage.DORegion.Get(),
		"do_endpoint":         config.Usage.DOEndpoint.Get(),
	}

	if bucket := config.Usage.DOBucket.Get(); bucket != "" {
		logFields["do_bucket"] = bucket
	} else if config.RuntimeOverrides.Enabled {
		logFields["do_bucket"] = "[pending runtime overrides]"
	}

	if prefix := config.Usage.DOPrefix.Get(); prefix != "" {
		logFields["do_prefix"] = prefix
	} else if config.RuntimeOverrides.Enabled {
		logFields["do_prefix"] = "[pending runtime overrides]"
	}

	logger.WithFields(logFields).Info("initializing usage-do module with configuration")

	return nil
}

func (m *module) buildDOConfig(config *config.Config) common.StorageConfig {
	storageConfig := common.StorageConfig{
		NodeID: config.Cluster.Hostname,
	}

	if config.Usage.DOBucket != nil {
		storageConfig.Bucket = config.Usage.DOBucket.Get()
	}
	if config.Usage.DOPrefix != nil {
		storageConfig.Prefix = config.Usage.DOPrefix.Get()
	}
	if config.Usage.PolicyVersion != nil {
		storageConfig.Version = config.Usage.PolicyVersion.Get()
	}

	return storageConfig
}

func parseDOConfig(config *config.Config) error {
	doBucket := ""
	if v := os.Getenv("USAGE_DO_BUCKET"); v != "" {
		doBucket = v
	} else if config.Usage.DOBucket != nil {
		doBucket = config.Usage.DOBucket.Get()
	}
	config.Usage.DOBucket = runtime.NewDynamicValue(doBucket)

	doPrefix := ""
	if v := os.Getenv("USAGE_DO_PREFIX"); v != "" {
		doPrefix = v
	} else if config.Usage.DOPrefix != nil {
		doPrefix = config.Usage.DOPrefix.Get()
	}
	config.Usage.DOPrefix = runtime.NewDynamicValue(doPrefix)

	doRegion := ""
	if v := os.Getenv("USAGE_DO_REGION"); v != "" {
		doRegion = v
	} else if config.Usage.DORegion != nil {
		doRegion = config.Usage.DORegion.Get()
	}
	config.Usage.DORegion = runtime.NewDynamicValue(doRegion)

	doEndpoint := ""
	if v := os.Getenv("USAGE_DO_ENDPOINT"); v != "" {
		doEndpoint = v
	} else if config.Usage.DOEndpoint != nil {
		doEndpoint = config.Usage.DOEndpoint.Get()
	}
	config.Usage.DOEndpoint = runtime.NewDynamicValue(doEndpoint)

	return nil
}

// verify we implement the required interfaces
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
