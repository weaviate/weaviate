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
	"encoding/json"
	"fmt"
	"os"
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
	storageClient *storage.Client
	bucketName    string
	prefix        string
	nodeID        string
	logger        logrus.FieldLogger
	metrics       *common.Metrics
}

// NewGCSStorage creates a new GCS storage backend
func NewGCSStorage(ctx context.Context, logger logrus.FieldLogger, metrics *common.Metrics) (*GCSStorage, error) {
	options := []option.ClientOption{}

	if os.Getenv("CLUSTER_IN_LOCALHOST") != "" {
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

	client.SetRetry(storage.WithBackoff(gax.Backoff{
		Initial:    2 * time.Second, // Note: the client uses a jitter internally
		Max:        60 * time.Second,
		Multiplier: 3,
	}),
		storage.WithPolicy(storage.RetryAlways),
	)

	return &GCSStorage{
		storageClient: client,
		logger:        logger,
		metrics:       metrics,
	}, nil
}

// VerifyPermissions checks if the backend can access the storage location
func (g *GCSStorage) VerifyPermissions(ctx context.Context) error {
	if g.storageClient == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	g.logger.WithFields(logrus.Fields{
		"action": "verify_bucket_permissions",
		"bucket": g.bucketName,
		"prefix": g.prefix,
	}).Info("")

	// Skip IAM permission check for emulator testing environments
	if os.Getenv("CLUSTER_IN_LOCALHOST") != "" {
		g.logger.Info("bucket access verification ignored for emulator")
		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Create storage API client to test IAM permissions
	storageService, err := storageapi.NewService(timeoutCtx)
	if err != nil {
		return fmt.Errorf("failed to create storage API client: %w", err)
	}

	// Test IAM permissions by calling the testIamPermissions API
	// This tests if we have storage.objects.create permission without creating any objects
	permissions := []string{"storage.objects.create"}

	_, err = storageService.Buckets.TestIamPermissions(g.bucketName, permissions).Context(timeoutCtx).Do()
	if err != nil {
		return fmt.Errorf("IAM permission check failed for bucket %s: %w", g.bucketName, err)
	}

	g.logger.WithFields(logrus.Fields{
		"bucket":      g.bucketName,
		"prefix":      g.prefix,
		"permissions": permissions,
	}).Info("IAM permissions verified successfully")

	return nil
}

// UploadUsageData uploads the usage data to the storage backend
func (g *GCSStorage) UploadUsageData(ctx context.Context, usage *types.Report) error {
	if g.storageClient == nil {
		return fmt.Errorf("storage client is not initialized")
	}

	data, err := json.MarshalIndent(usage, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal usage data: %w", err)
	}

	// Create filename with timestamp only - keeping minutes and seconds sharp at 00
	now := time.Now().UTC()
	timestamp := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC).Format("2006-01-02T15-04-05Z")
	filename := fmt.Sprintf("%s.json", timestamp)

	gcsFilename := fmt.Sprintf("%s/%s", g.nodeID, filename)
	if g.prefix != "" {
		gcsFilename = fmt.Sprintf("%s/%s/%s", g.prefix, g.nodeID, filename)
	}

	obj := g.storageClient.Bucket(g.bucketName).Object(gcsFilename)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"
	writer.Metadata = map[string]string{
		"timestamp": timestamp,
		"version":   usage.Version,
	}

	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	// Set uploaded file size metric
	g.metrics.UploadedFileSize.Set(float64(len(data)))

	return nil
}

// Close cleans up any resources used by the storage backend
func (g *GCSStorage) Close() error {
	if g.storageClient != nil {
		return g.storageClient.Close()
	}
	return nil
}

// UpdateConfig updates the backend configuration from the provided config
func (g *GCSStorage) UpdateConfig(config common.StorageConfig) (bool, error) {
	changed := false
	// Check for bucket name changes
	if config.Bucket != "" && g.bucketName != config.Bucket {
		g.logger.WithFields(logrus.Fields{
			"old_bucket": g.bucketName,
			"new_bucket": config.Bucket,
		}).Warn("bucket name changed - this may require re-authentication")
		g.bucketName = config.Bucket
		changed = true
	}

	// Check for prefix changes
	if g.prefix != config.Prefix {
		g.logger.WithFields(logrus.Fields{
			"old_prefix": g.prefix,
			"new_prefix": config.Prefix,
		}).Info("upload prefix updated")
		g.prefix = config.Prefix
		changed = true
	}

	return changed, nil
}

// verify we implement the required interfaces
var (
	gcsStorage, _ = NewGCSStorage(context.Background(), logrus.New(), &common.Metrics{})
	_             = common.StorageBackend(gcsStorage)
)
