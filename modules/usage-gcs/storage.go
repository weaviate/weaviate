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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/googleapis/gax-go/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	storageapi "google.golang.org/api/storage/v1"

	"github.com/weaviate/weaviate/cluster/usage/types"
	common "github.com/weaviate/weaviate/usecases/modulecomponents/usage"
)

// GCSStorage implements the StorageBackend interface for GCS
type GCSStorage struct {
	*common.BaseStorage
	storageService *storageapi.Service
}

// NewGCSStorage creates a new GCS storage backend
func NewGCSStorage(ctx context.Context, logger logrus.FieldLogger, metrics *common.Metrics) (*GCSStorage, error) {
	options := []option.ClientOption{}

	// Use base storage localhost check for authentication
	baseStorage := common.NewBaseStorage(logger, metrics)

	if baseStorage.IsLocalhostEnvironment() {
		options = append(options, option.WithoutAuthentication())
	}

	client, err := storageapi.NewService(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP storage client: %w", err)
	}

	return &GCSStorage{
		BaseStorage:    baseStorage,
		storageService: client,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (g *GCSStorage) VerifyPermissions(ctx context.Context) error {
	if g.storageService == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	// During initialization, bucket may not be configured yet due to runtime overrides
	// being loaded after module initialization.
	if g.BucketName == "" {
		g.Logger.Debug("GCS bucket not configured yet - skipping permission verification")
		return nil
	}

	g.LogVerificationStart()

	if g.IsLocalhostEnvironment() {
		return nil
	}

	// Create context with timeout to report early in case of invalid permissions
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// GCS-specific permission check using IAM
	permissions := []string{"storage.objects.create"}
	_, err := g.storageService.Buckets.TestIamPermissions(g.BucketName, permissions).Context(timeoutCtx).Do()
	if err != nil {
		return fmt.Errorf("IAM permission check failed for bucket %s: %w", g.BucketName, err)
	}

	g.LogVerificationSuccess(logrus.Fields{
		"permissions": permissions,
	})
	return nil
}

// UploadUsageData uploads the usage data to the storage backend
func (g *GCSStorage) UploadUsageData(ctx context.Context, usage *types.Report) error {
	if g.storageService == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	data, err := g.MarshalUsageData(usage)
	if err != nil {
		return err
	}

	objectName := g.ConstructObjectKey(usage.CollectingTime)

	_, err = g.storageService.Objects.Insert(g.BucketName, &storageapi.Object{
		Name:        objectName,
		ContentType: "application/json",
		Metadata: map[string]string{
			"version": usage.Version,
		},
	}).WithRetry(&gax.Backoff{
		Initial:    2 * time.Second, // Note: the client uses a jitter internally
		Max:        60 * time.Second,
		Multiplier: 3,
	}, func(err error) bool {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			g.Logger.WithField("gcs_upload", "retry").Debugf("retrying due to code: %v", gerr.Code)
			// retry only on those given error codes
			return gerr.Code == 401 || gerr.Code == 429 || (gerr.Code >= 500 && gerr.Code < 600)
		}
		return false
	}).Media(bytes.NewReader(data)).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("failed to upload to GCS: %w", err)
	}

	g.RecordUploadMetrics(len(data))
	return nil
}

// Close cleans up resources
func (g *GCSStorage) Close() error {
	// storageapi.Service doesn't have a Close method
	// requests are handled by the HTTP client
	// see https://github.com/googleapis/google-cloud-go/blob/storage/v1.56.1/storage/storage.go#L154-L161
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (g *GCSStorage) UpdateConfig(config common.StorageConfig) (bool, error) {
	// Store old bucket name to detect changes
	oldBucketName := g.BucketName

	// Update the configuration
	configChanged := g.UpdateCommonConfig(config)
	if !configChanged {
		return configChanged, nil
	}

	// If bucket name changed, verify permissions
	if oldBucketName != g.BucketName {
		g.Logger.WithFields(logrus.Fields{
			"old_bucket": oldBucketName,
			"new_bucket": g.BucketName,
		}).Info("GCS bucket name changed")
	}

	if !config.VerifyPermissions {
		g.Logger.Info("permission verification skipped after bucket change (disabled by configuration)")
		return configChanged, nil
	}

	g.Logger.Info("verifying permissions")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := g.VerifyPermissions(ctx); err != nil {
		g.Logger.WithError(err).Error("GCS permission verification failed after bucket change")
		return configChanged, err
	}
	g.Logger.Info("GCS permissions verified successfully")

	return configChanged, nil
}

// verify we implement the required interfaces
var (
	gcsStorage, _ = NewGCSStorage(context.Background(), logrus.New(), &common.Metrics{})
	_             = common.StorageBackend(gcsStorage)
)
