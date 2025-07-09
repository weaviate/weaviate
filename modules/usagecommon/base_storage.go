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

package usagecommon

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/usage/types"
)

// BaseStorage contains common fields and methods for all storage backends
type BaseStorage struct {
	BucketName string
	Prefix     string
	NodeID     string
	Logger     logrus.FieldLogger
	Metrics    *Metrics
}

// NewBaseStorage creates a new base storage instance
func NewBaseStorage(logger logrus.FieldLogger, metrics *Metrics) *BaseStorage {
	return &BaseStorage{
		Logger:  logger,
		Metrics: metrics,
	}
}

// IsLocalhostEnvironment checks if we're running in a localhost/emulator environment
func (b *BaseStorage) IsLocalhostEnvironment() bool {
	return os.Getenv("CLUSTER_IN_LOCALHOST") != ""
}

// ConstructObjectKey builds the full object key with prefix and nodeID
func (b *BaseStorage) ConstructObjectKey() string {
	now := time.Now().UTC()
	timestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC).Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s.json", timestamp)

	objectKey := fmt.Sprintf("%s/%s", b.NodeID, filename)
	if b.Prefix != "" {
		objectKey = fmt.Sprintf("%s/%s/%s", b.Prefix, b.NodeID, filename)
	}
	return objectKey
}

// MarshalUsageData converts usage data to JSON
func (b *BaseStorage) MarshalUsageData(usage *types.Report) ([]byte, error) {
	data, err := json.MarshalIndent(usage, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal usage data: %w", err)
	}
	return data, nil
}

// UpdateCommonConfig handles common configuration updates
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

	// Check for nodeID changes
	if config.NodeID != "" && b.NodeID != config.NodeID {
		b.NodeID = config.NodeID
		changed = true
	}

	return changed
}

// LogVerificationStart logs the start of permission verification
func (b *BaseStorage) LogVerificationStart() {
	b.Logger.WithFields(logrus.Fields{
		"action": "verify_bucket_permissions",
		"bucket": b.BucketName,
		"prefix": b.Prefix,
	}).Info("")
}

// LogVerificationSkipped logs when verification is skipped for localhost
func (b *BaseStorage) LogVerificationSkipped(reason string) {
	b.Logger.WithFields(logrus.Fields{
		"reason": reason,
	}).Info("bucket access verification ignored")
}

// LogVerificationSuccess logs successful permission verification
func (b *BaseStorage) LogVerificationSuccess(additionalFields ...logrus.Fields) {
	fields := logrus.Fields{
		"bucket": b.BucketName,
		"prefix": b.Prefix,
	}

	// Merge additional fields if provided
	for _, additional := range additionalFields {
		for k, v := range additional {
			fields[k] = v
		}
	}

	b.Logger.WithFields(fields).Info("permissions verified successfully")
}

// RecordUploadMetrics updates the uploaded file size metric
func (b *BaseStorage) RecordUploadMetrics(dataSize int) {
	b.Metrics.UploadedFileSize.Set(float64(dataSize))
}
