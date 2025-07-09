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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/usage/types"
	common "github.com/weaviate/weaviate/modules/usagecommon"
)

// S3Storage implements the StorageBackend interface for S3
type S3Storage struct {
	s3Client   *s3.Client
	bucketName string
	prefix     string
	nodeID     string
	logger     logrus.FieldLogger
	metrics    *common.Metrics
}

// NewS3Storage creates a new S3 storage backend
func NewS3Storage(logger logrus.FieldLogger, metrics *common.Metrics) (*S3Storage, error) {
	// Load default AWS configuration (handles region, credentials automatically)
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with MinIO endpoint support
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Handle custom endpoint for MinIO
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			// Add protocol if missing
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint // Default to HTTP for localhost
			}
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Required for MinIO compatibility
		}
	})

	return &S3Storage{
		s3Client: s3Client,
		logger:   logger,
		metrics:  metrics,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (s *S3Storage) VerifyPermissions(ctx context.Context) error {
	if s.s3Client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	s.logger.WithFields(logrus.Fields{
		"action": "verify_bucket_permissions",
		"bucket": s.bucketName,
		"prefix": s.prefix,
	}).Info("")

	// Skip permission check for local testing environments
	if os.Getenv("CLUSTER_IN_LOCALHOST") != "" {
		s.logger.Info("bucket access verification ignored for localhost")
		return nil
	}

	// Create context with timeout for permission check
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Test bucket access by listing objects (limited to 1)
	_, err := s.s3Client.ListObjectsV2(timeoutCtx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucketName),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("S3 permission check failed for bucket %s: %w", s.bucketName, err)
	}

	s.logger.WithFields(logrus.Fields{
		"bucket": s.bucketName,
		"prefix": s.prefix,
	}).Info("S3 permissions verified successfully")

	return nil
}

// UploadUsageData uploads the usage data to the storage backend
func (s *S3Storage) UploadUsageData(ctx context.Context, usage *types.Report) error {
	if s.s3Client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	data, err := json.MarshalIndent(usage, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal usage data: %w", err)
	}

	// Create filename with timestamp only - keeping minutes and seconds sharp at 00
	now := time.Now().UTC()
	timestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC).Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s.json", timestamp)

	s3Key := fmt.Sprintf("%s/%s", s.nodeID, filename)
	if s.prefix != "" {
		s3Key = fmt.Sprintf("%s/%s/%s", s.prefix, s.nodeID, filename)
	}

	// Upload to S3
	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(s3Key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		Metadata: map[string]string{
			"timestamp": timestamp,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	// Set uploaded file size metric
	s.metrics.UploadedFileSize.Set(float64(len(data)))

	return nil
}

// Close cleans up any resources used by the storage backend
func (s *S3Storage) Close() error {
	// S3 client doesn't need explicit closing
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (s *S3Storage) UpdateConfig(config common.StorageConfig) (bool, error) {
	changed := false

	// Check for bucket name changes
	if config.Bucket != "" && s.bucketName != config.Bucket {
		s.logger.WithFields(logrus.Fields{
			"old_bucket": s.bucketName,
			"new_bucket": config.Bucket,
		}).Warn("bucket name changed - this may require re-authentication")
		s.bucketName = config.Bucket
		changed = true
	}

	// Check for prefix changes
	if s.prefix != config.Prefix {
		s.logger.WithFields(logrus.Fields{
			"old_prefix": s.prefix,
			"new_prefix": config.Prefix,
		}).Info("upload prefix updated")
		s.prefix = config.Prefix
		changed = true
	}

	return changed, nil
}

// verify we implement the required interfaces
var (
	storage, _ = NewS3Storage(logrus.New(), &common.Metrics{})
	_          = common.StorageBackend(storage)
)
