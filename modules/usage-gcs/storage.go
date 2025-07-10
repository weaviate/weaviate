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
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	storageapi "google.golang.org/api/storage/v1"

	"github.com/weaviate/weaviate/cluster/usage/types"
	common "github.com/weaviate/weaviate/modules/usagecommon"
)

// GCSStorage implements the StorageBackend interface for GCS
type GCSStorage struct {
	*common.BaseStorage
	storageClient *storage.Client
}

// NewGCSStorage creates a new GCS storage backend
func NewGCSStorage(ctx context.Context, logger logrus.FieldLogger, metrics *common.Metrics) (*GCSStorage, error) {
	options := []option.ClientOption{}

	// Use base storage localhost check for authentication
	baseStorage := common.NewBaseStorage(logger, metrics)

	if baseStorage.IsLocalhostEnvironment() {
		options = append(options, option.WithoutAuthentication())
	} else {
		scopes := []string{
			"https://www.googleapis.com/auth/devstorage.read_write",
		}
		creds, err := google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, errors.Wrap(err, "find default credentials")
		}
		options = append(options, option.WithCredentials(creds))
	}

	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP storage client: %w", err)
	}

	// Configure retry policy
	client.SetRetry(storage.WithBackoff(gax.Backoff{
		Initial:    2 * time.Second,
		Max:        60 * time.Second,
		Multiplier: 3,
	}),
		storage.WithPolicy(storage.RetryAlways),
	)

	return &GCSStorage{
		BaseStorage:   baseStorage,
		storageClient: client,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (g *GCSStorage) VerifyPermissions(ctx context.Context) error {
	if g.storageClient == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	// Check if bucket name is configured
	if g.BucketName == "" {
		return fmt.Errorf("GCS bucket name is not configured - cannot verify permissions")
	}

	g.LogVerificationStart()

	if g.IsLocalhostEnvironment() {
		return nil
	}

	// Create context with timeout to report early in case of invalid permissions
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// GCS-specific permission check using IAM
	storageService, err := storageapi.NewService(timeoutCtx)
	if err != nil {
		return fmt.Errorf("failed to create storage API client: %w", err)
	}

	permissions := []string{"storage.objects.create"}
	_, err = storageService.Buckets.TestIamPermissions(g.BucketName, permissions).Context(timeoutCtx).Do()
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
	if g.storageClient == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	data, err := g.MarshalUsageData(usage)
	if err != nil {
		return err
	}

	obj := g.storageClient.Bucket(g.BucketName).Object(g.ConstructObjectKey())
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"
	writer.Metadata = map[string]string{
		"version": usage.Version,
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	g.RecordUploadMetrics(len(data))
	return nil
}

// Close cleans up resources
func (g *GCSStorage) Close() error {
	if g.storageClient != nil {
		return g.storageClient.Close()
	}
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (g *GCSStorage) UpdateConfig(config common.StorageConfig) (bool, error) {
	return g.UpdateCommonConfig(config), nil
}

// verify we implement the required interfaces
var (
	gcsStorage, _ = NewGCSStorage(context.Background(), logrus.New(), &common.Metrics{})
	_             = common.StorageBackend(gcsStorage)
)
