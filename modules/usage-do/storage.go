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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/usage/types"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
)

// DOStorage implements the StorageBackend interface for DigitalOcean Spaces
type DOStorage struct {
	*common.BaseStorage
	s3Client *s3.Client
}

// NewDOStorage creates a new DigitalOcean Spaces storage backend.
// DigitalOcean Spaces is S3-compatible but requires an explicit region and
// endpoint in the format https://{region}.digitaloceanspaces.com.
func NewDOStorage(ctx context.Context, logger logrus.FieldLogger, metrics *common.Metrics, region, endpoint string) (*DOStorage, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return &DOStorage{
		BaseStorage: common.NewBaseStorage(logger, metrics),
		s3Client:    s3Client,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (s *DOStorage) VerifyPermissions(ctx context.Context) error {
	if s.s3Client == nil {
		return fmt.Errorf("DigitalOcean Spaces client is not initialized")
	}

	// During initialization, bucket may not be configured yet due to runtime overrides
	// being loaded after module initialization.
	if s.BucketName == "" {
		s.Logger.Debug("DigitalOcean Spaces bucket not configured yet - skipping permission verification")
		return nil
	}

	s.LogVerificationStart()

	if s.IsLocalhostEnvironment() {
		return nil
	}

	// Create context with timeout to report early in case of invalid permissions
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := s.s3Client.ListObjectsV2(timeoutCtx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.BucketName),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("DigitalOcean Spaces permission check failed for bucket %s: %w", s.BucketName, err)
	}

	s.LogVerificationSuccess()
	return nil
}

// UploadUsageData uploads the usage data to the storage backend
func (s *DOStorage) UploadUsageData(ctx context.Context, usage *types.Report) error {
	if s.s3Client == nil {
		return fmt.Errorf("DigitalOcean Spaces client is not initialized")
	}

	data, err := s.MarshalUsageData(usage)
	if err != nil {
		return err
	}

	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.BucketName),
		Key:         aws.String(s.ConstructObjectKey(usage.CollectingTime)),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		Metadata: map[string]string{
			"version": usage.Version,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload to DigitalOcean Spaces: %w", err)
	}

	s.RecordUploadMetrics(len(data))
	return nil
}

// Close cleans up resources
func (s *DOStorage) Close() error {
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (s *DOStorage) UpdateConfig(config common.StorageConfig) (bool, error) {
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
		}).Info("DigitalOcean Spaces bucket name changed")
	}

	if !config.VerifyPermissions {
		s.Logger.Info("permission verification skipped after bucket change (disabled by configuration)")
		return configChanged, nil
	}

	s.Logger.Info("verifying permissions")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.VerifyPermissions(ctx); err != nil {
		s.Logger.WithError(err).Error("DigitalOcean Spaces permission verification failed after bucket change")
		return configChanged, err
	}
	s.Logger.Info("DigitalOcean Spaces permissions verified successfully")

	return configChanged, nil
}

// verify we implement the required interfaces
var (
	doStorage, _ = NewDOStorage(context.Background(), logrus.New(), &common.Metrics{}, "us-east-1", "https://nyc3.digitaloceanspaces.com")
	_            = common.StorageBackend(doStorage)
)
