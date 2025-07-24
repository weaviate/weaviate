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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/usage/types"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
)

// S3Storage implements the StorageBackend interface for S3
type S3Storage struct {
	*common.BaseStorage
	s3Client *s3.Client
}

// NewS3Storage creates a new S3 storage backend
func NewS3Storage(ctx context.Context, logger logrus.FieldLogger, metrics *common.Metrics) (*S3Storage, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
				endpoint = "http://" + endpoint
			}
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	return &S3Storage{
		BaseStorage: common.NewBaseStorage(logger, metrics),
		s3Client:    s3Client,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (s *S3Storage) VerifyPermissions(ctx context.Context) error {
	if s.s3Client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	// During initialization, bucket may not be configured yet due to runtime overrides
	// being loaded after module initialization.
	if s.BucketName == "" {
		s.Logger.Debug("S3 bucket not configured yet - skipping permission verification")
		return nil
	}

	s.LogVerificationStart()

	if s.IsLocalhostEnvironment() {
		return nil
	}

	// Create context with timeout to report early in case of invalid permissions
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// S3-specific permission check
	_, err := s.s3Client.ListObjectsV2(timeoutCtx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.BucketName),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("S3 permission check failed for bucket %s: %w", s.BucketName, err)
	}

	s.LogVerificationSuccess()
	return nil
}

// UploadUsageData uploads the usage data to the storage backend
func (s *S3Storage) UploadUsageData(ctx context.Context, usage *types.Report) error {
	if s.s3Client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	data, err := s.MarshalUsageData(usage)
	if err != nil {
		return err
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.BucketName),
		Key:         aws.String(s.ConstructObjectKey(usage.CollectingTIme)),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		Metadata: map[string]string{
			"version": usage.Version,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	s.RecordUploadMetrics(len(data))
	return nil
}

// Close cleans up resources
func (s *S3Storage) Close() error {
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (s *S3Storage) UpdateConfig(config common.StorageConfig) (bool, error) {
	// Store old bucket name to detect changes
	oldBucketName := s.BucketName

	// Update the configuration
	configChanged := s.UpdateCommonConfig(config)

	if !configChanged {
		return configChanged, nil
	}

	// If bucket name changed, verify permissions
	if oldBucketName != s.BucketName {
		s.Logger.WithFields(logrus.Fields{
			"old_bucket": oldBucketName,
			"new_bucket": s.BucketName,
		}).Info("S3 bucket name changed")
	}

	if !config.VerifyPermissions {
		s.Logger.Info("permission verification skipped after bucket change (disabled by configuration)")
		return configChanged, nil
	}

	s.Logger.Info("verifying permissions")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.VerifyPermissions(ctx); err != nil {
		s.Logger.WithError(err).Error("S3 permission verification failed after bucket change")
		return configChanged, err
	}
	s.Logger.Info("S3 permissions verified successfully")

	return configChanged, nil
}

// verify we implement the required interfaces
var (
	s3Storage, _ = NewS3Storage(context.Background(), logrus.New(), &common.Metrics{})
	_            = common.StorageBackend(s3Storage)
)
