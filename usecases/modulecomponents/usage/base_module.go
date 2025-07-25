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

package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

const (
	DefaultCollectionInterval = 1 * time.Hour
	// DefaultShardJitterInterval short for shard-level operations and can be configurable later on
	DefaultShardJitterInterval = 100 * time.Millisecond
	DefaultRuntimeLoadInterval = 2 * time.Minute
	DefaultPolicyVersion       = "2025-06-01"
)

// BaseModule contains the common logic for usage collection modules
type BaseModule struct {
	nodeID        string
	policyVersion string
	moduleName    string
	config        *config.Config
	storage       StorageBackend
	interval      time.Duration
	shardJitter   time.Duration
	stopChan      chan struct{}
	metrics       *Metrics
	usageService  clusterusage.Service
	logger        logrus.FieldLogger
}

// NewBaseModule creates a new base module instance
func NewBaseModule(moduleName string, storage StorageBackend) *BaseModule {
	return &BaseModule{
		interval:    DefaultCollectionInterval,
		shardJitter: DefaultShardJitterInterval,
		stopChan:    make(chan struct{}),
		storage:     storage,
		moduleName:  moduleName,
	}
}

func (b *BaseModule) SetUsageService(usageService any) {
	if service, ok := usageService.(clusterusage.Service); ok {
		b.usageService = service
		service.SetJitterInterval(b.shardJitter)
	}
}

func (b *BaseModule) Name() string {
	return b.moduleName
}

func (b *BaseModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Usage
}

// InitializeCommon initializes the common components of the usage module
func (b *BaseModule) InitializeCommon(ctx context.Context, config *config.Config, logger logrus.FieldLogger, metrics *Metrics) error {
	b.config = config
	b.logger = logger.WithField("component", b.moduleName)
	b.metrics = metrics
	if b.config.Cluster.Hostname == "" {
		return fmt.Errorf("cluster hostname is not set")
	}

	b.nodeID = b.config.Cluster.Hostname

	// Initialize policy version
	if b.config.Usage.PolicyVersion != nil {
		b.policyVersion = b.config.Usage.PolicyVersion.Get()
	}
	if b.policyVersion == "" {
		b.policyVersion = DefaultPolicyVersion
	}

	if b.config.Usage.ScrapeInterval != nil {
		if interval := b.config.Usage.ScrapeInterval.Get(); interval > 0 {
			b.interval = interval
		}
	}

	// Initialize shard jitter interval
	if b.config.Usage.ShardJitterInterval != nil {
		if jitterInterval := b.config.Usage.ShardJitterInterval.Get(); jitterInterval > 0 {
			b.shardJitter = jitterInterval
		}
	}

	// Verify storage permissions (opt-in)
	var shouldVerifyPermissions bool
	if b.config.Usage.VerifyPermissions != nil {
		shouldVerifyPermissions = b.config.Usage.VerifyPermissions.Get()
	}

	if shouldVerifyPermissions {
		if err := b.storage.VerifyPermissions(ctx); err != nil {
			return fmt.Errorf("failed to verify storage permissions: %w", err)
		}
		b.logger.Info("storage permissions verified successfully")
	} else {
		b.logger.Info("storage permission verification skipped (disabled by configuration)")
	}

	// Start periodic collection and upload
	enterrors.GoWrapper(func() {
		b.collectAndUploadPeriodically(context.Background())
	}, b.logger)

	b.logger.Infof("%s module initialized successfully", b.moduleName)
	return nil
}

func (b *BaseModule) collectAndUploadPeriodically(ctx context.Context) {
	// Validate intervals before creating tickers
	if b.interval <= 0 {
		b.logger.Warn("Invalid collection interval (<= 0), using default of 1 hour")
		b.interval = DefaultCollectionInterval
	}

	loadInterval := b.config.RuntimeOverrides.LoadInterval
	if loadInterval <= 0 {
		b.logger.Warn("Invalid runtime overrides load interval (<= 0), using default of 2 minutes")
		loadInterval = DefaultRuntimeLoadInterval
	}

	b.logger.WithFields(logrus.Fields{
		"base_interval":        b.interval.String(),
		"load_interval":        loadInterval.String(),
		"shard_jitter":         b.shardJitter.String(),
		"default_shard_jitter": DefaultShardJitterInterval.String(),
	}).Debug("starting periodic collection with ticker")

	// Create ticker with base interval
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	loadTicker := time.NewTicker(loadInterval)
	defer loadTicker.Stop()

	b.logger.WithFields(logrus.Fields{
		"interval":       b.interval.String(),
		"ticker_created": time.Now(),
		"next_fire":      time.Now().Add(b.interval),
	}).Debug("ticker created successfully, entering main loop")

	for {
		select {
		case <-ticker.C:
			b.logger.WithFields(logrus.Fields{
				"current_time": time.Now(),
			}).Debug("ticker fired - starting collection cycle")

			if err := b.collectAndUploadUsage(ctx); err != nil {
				b.logger.WithError(err).Error("Failed to collect and upload usage data")
				b.metrics.OperationTotal.WithLabelValues("collect_and_upload", "error").Inc()
			} else {
				b.metrics.OperationTotal.WithLabelValues("collect_and_upload", "success").Inc()
			}
			// ticker is used to reset the interval
			b.reloadConfig(ticker)

		case <-loadTicker.C:
			b.logger.Debug("runtime overrides reloaded")
			// ticker is used to reset the interval
			b.reloadConfig(ticker)

		case <-ctx.Done():
			b.logger.WithFields(logrus.Fields{"error": ctx.Err()}).Info("context cancelled - stopping periodic collection")
			return
		case <-b.stopChan:
			b.logger.Info("stop signal received - stopping periodic collection")
			return
		}
	}
}

func (b *BaseModule) collectAndUploadUsage(ctx context.Context) error {
	// Collect usage data and update metrics
	usage, err := b.collectUsageData(ctx)
	if err != nil {
		return err
	}

	// Set version on usage data
	usage.Version = b.policyVersion

	// Upload the collected data
	return b.storage.UploadUsageData(ctx, usage)
}

func (b *BaseModule) collectUsageData(ctx context.Context) (*types.Report, error) {
	if b.usageService == nil {
		return nil, fmt.Errorf("usage service not initialized")
	}

	usage, err := b.usageService.Usage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get usage data: %w", err)
	}

	// Compute total collections and update gauge.
	totalCollections := float64(len(usage.Collections))
	b.metrics.ResourceCount.WithLabelValues("collections").Set(totalCollections)

	// Compute total shards and update gauge.
	var totalShards float64
	for _, coll := range usage.Collections {
		totalShards += float64(coll.UniqueShardCount)
	}
	b.metrics.ResourceCount.WithLabelValues("shards").Set(totalShards)

	// Compute total backups and update gauge.
	totalBackups := float64(len(usage.Backups))
	b.metrics.ResourceCount.WithLabelValues("backups").Set(totalBackups)

	return usage, nil
}

func (b *BaseModule) reloadConfig(ticker *time.Ticker) {
	// Check for interval updates
	if interval := b.config.Usage.ScrapeInterval.Get(); interval > 0 && b.interval != interval {
		b.logger.WithFields(logrus.Fields{
			"old_interval": b.interval.String(),
			"new_interval": interval.String(),
		}).Info("collection interval updated")
		b.interval = interval
		// Reset ticker with new interval
		ticker.Reset(b.interval)
	} else if interval <= 0 && b.interval <= 0 {
		// If both old and new intervals are invalid, set a default
		b.logger.Warn("Invalid interval detected during reload, using default of 1 hour")
		b.interval = DefaultCollectionInterval
		ticker.Reset(b.interval)
	}

	// Check for shard jitter interval updates
	// Note: we allow 0 as a valid value for the shard jitter interval
	if jitterInterval := b.config.Usage.ShardJitterInterval.Get(); jitterInterval >= 0 && b.shardJitter != jitterInterval {
		b.logger.WithFields(logrus.Fields{
			"old_jitter": b.shardJitter.String(),
			"new_jitter": jitterInterval.String(),
		}).Info("shard jitter interval updated")
		b.shardJitter = jitterInterval
		b.usageService.SetJitterInterval(b.shardJitter)
	}

	// Build common storage config
	storageConfig := b.buildStorageConfig()

	// Update storage backend configuration
	if changed, err := b.storage.UpdateConfig(storageConfig); err != nil {
		b.logger.WithError(err).Error("Failed to update storage configuration")
	} else if changed {
		b.logger.Info("storage configuration updated")
	}
}

func (b *BaseModule) buildStorageConfig() StorageConfig {
	config := StorageConfig{
		NodeID:            b.nodeID,
		Version:           b.policyVersion,
		VerifyPermissions: false,
	}

	// Set verification setting from configuration
	if b.config.Usage.VerifyPermissions != nil {
		config.VerifyPermissions = b.config.Usage.VerifyPermissions.Get()
	}

	if b.config.Usage.S3Bucket != nil {
		config.Bucket = b.config.Usage.S3Bucket.Get()
	}
	if b.config.Usage.S3Prefix != nil {
		config.Prefix = b.config.Usage.S3Prefix.Get()
	}

	if b.config.Usage.GCSBucket != nil {
		config.Bucket = b.config.Usage.GCSBucket.Get()
	}

	if b.config.Usage.GCSPrefix != nil {
		config.Prefix = b.config.Usage.GCSPrefix.Get()
	}

	return config
}

func (b *BaseModule) Close() error {
	close(b.stopChan)
	if b.storage != nil {
		return b.storage.Close()
	}
	return nil
}

func (b *BaseModule) Logger() logrus.FieldLogger {
	return b.logger
}

func (b *BaseModule) GetMetrics() *Metrics {
	return b.metrics
}
