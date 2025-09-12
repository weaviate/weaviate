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
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/usage/types"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// BaseStorage provides common functionality for all storage backends
type BaseStorage struct {
	BucketName        string
	Prefix            string
	NodeID            string
	VerifyPermissions bool
	Logger            logrus.FieldLogger
	Metrics           *Metrics
}

// NewBaseStorage creates a new base storage instance
func NewBaseStorage(logger logrus.FieldLogger, metrics *Metrics) *BaseStorage {
	return &BaseStorage{
		Logger:  logger,
		Metrics: metrics,
	}
}

// ConstructObjectKey creates the full object key path for storage
func (b *BaseStorage) ConstructObjectKey(collectionTime string) string {
	filename := fmt.Sprintf("%s.json", collectionTime)

	objectKey := fmt.Sprintf("%s/%s", b.NodeID, filename)
	if b.Prefix != "" {
		objectKey = fmt.Sprintf("%s/%s/%s", b.Prefix, b.NodeID, filename)
	}
	return objectKey
}

// MarshalUsageData converts usage data to JSON
func (b *BaseStorage) MarshalUsageData(usage *types.Report) ([]byte, error) {
	data, err := json.Marshal(usage)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal usage data: %w", err)
	}
	return data, nil
}

// UpdateCommonConfig updates common configuration fields and returns whether anything changed
func (b *BaseStorage) UpdateCommonConfig(config StorageConfig) bool {
	changed := false

	// Check for bucket name changes
	if config.Bucket != "" && b.BucketName != config.Bucket {
		b.Logger.WithFields(logrus.Fields{
			"old_bucket": b.BucketName,
			"new_bucket": config.Bucket,
		}).Warn("bucket name changed - this may require re-authentication")
		b.BucketName = config.Bucket
		changed = true
	}

	// Check for prefix changes
	if b.Prefix != config.Prefix {
		b.Logger.WithFields(logrus.Fields{
			"old_prefix": b.Prefix,
			"new_prefix": config.Prefix,
		}).Info("upload prefix updated")
		b.Prefix = config.Prefix
		changed = true
	}

	if b.VerifyPermissions != config.VerifyPermissions {
		b.Logger.WithFields(logrus.Fields{
			"old_verify_permissions": b.VerifyPermissions,
			"new_verify_permissions": config.VerifyPermissions,
		}).Info("verify permissions updated")
		b.VerifyPermissions = config.VerifyPermissions
		changed = true
	}

	// Check for nodeID changes
	if config.NodeID != "" && b.NodeID != config.NodeID {
		b.Logger.WithFields(logrus.Fields{
			"old_node_id": b.NodeID,
			"new_node_id": config.NodeID,
		}).Info("node ID updated")
		b.NodeID = config.NodeID
		changed = true
	}

	return changed
}

// IsLocalhostEnvironment checks if running in localhost/emulator mode
func (b *BaseStorage) IsLocalhostEnvironment() bool {
	return os.Getenv("CLUSTER_IN_LOCALHOST") != ""
}

// LogVerificationStart logs the start of permission verification
func (b *BaseStorage) LogVerificationStart() {
	b.Logger.WithFields(logrus.Fields{
		"action": "verify_bucket_permissions",
		"bucket": b.BucketName,
		"prefix": b.Prefix,
	}).Info("")
}

// LogVerificationSuccess logs successful permission verification
func (b *BaseStorage) LogVerificationSuccess(extraFields ...logrus.Fields) {
	fields := logrus.Fields{
		"bucket": b.BucketName,
		"prefix": b.Prefix,
	}

	// Merge any extra fields provided
	for _, extra := range extraFields {
		for k, v := range extra {
			fields[k] = v
		}
	}

	b.Logger.WithFields(fields).Info("permissions verified successfully")
}

// RecordUploadMetrics records upload metrics
func (b *BaseStorage) RecordUploadMetrics(dataSize int) {
	if b.Metrics != nil && b.Metrics.UploadedFileSize != nil {
		b.Metrics.UploadedFileSize.Set(float64(dataSize))
	}
}

// ParseCommonUsageConfig parses common environment variables shared by all usage modules
func ParseCommonUsageConfig(config *config.Config) error {
	// Parse common environment variables that both S3 and GCS modules use
	scrapeInterval := DefaultCollectionInterval
	if v := os.Getenv("USAGE_SCRAPE_INTERVAL"); v != "" {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", "USAGE_SCRAPE_INTERVAL", err)
		}
		scrapeInterval = duration
	} else if config.Usage.ScrapeInterval != nil {
		scrapeInterval = config.Usage.ScrapeInterval.Get()
	}
	config.Usage.ScrapeInterval = runtime.NewDynamicValue(scrapeInterval)

	policyVersion := build.Version
	if v := os.Getenv("USAGE_POLICY_VERSION"); v != "" {
		policyVersion = v
	} else if config.Usage.PolicyVersion != nil {
		policyVersion = config.Usage.PolicyVersion.Get()
	}
	config.Usage.PolicyVersion = runtime.NewDynamicValue(policyVersion)

	// Parse shard jitter interval environment variable
	shardJitterInterval := DefaultShardJitterInterval
	if v := os.Getenv("USAGE_SHARD_JITTER_INTERVAL"); v != "" {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", "USAGE_SHARD_JITTER_INTERVAL", err)
		}
		shardJitterInterval = duration
	} else if config.Usage.ShardJitterInterval != nil {
		shardJitterInterval = config.Usage.ShardJitterInterval.Get()
	}
	config.Usage.ShardJitterInterval = runtime.NewDynamicValue(shardJitterInterval)

	// Parse verify permissions setting
	verifyPermissions := false
	if v := os.Getenv("USAGE_VERIFY_PERMISSIONS"); v != "" {
		verifyPermissions = entcfg.Enabled(v)
	} else if config.Usage.VerifyPermissions != nil {
		verifyPermissions = config.Usage.VerifyPermissions.Get()
	}
	config.Usage.VerifyPermissions = runtime.NewDynamicValue(verifyPermissions)

	return nil
}
