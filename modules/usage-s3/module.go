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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"

	clusterusage "github.com/weaviate/weaviate/cluster/usage"
	"github.com/weaviate/weaviate/cluster/usage/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	Name                       = "usage-s3"
	DefaultCollectionInterval  = 1 * time.Hour
	DefaultJitterInterval      = 30 * time.Second
	DefaultVersion             = "2025-06-01" // TODO: update this to the actual version
	DefaultRuntimeLoadInterval = 2 * time.Minute
)

// module handles collecting and uploading usage metrics to S3
type module struct {
	config        *config.Config
	logger        logrus.FieldLogger
	s3Client      *s3.S3
	bucketName    string
	prefix        string
	interval      time.Duration
	stopChan      chan struct{}
	nodeID        string
	metrics       *metrics
	policyVersion string
	usageService  clusterusage.Service
}

func New() *module {
	return &module{
		interval: DefaultCollectionInterval,
		stopChan: make(chan struct{}),
	}
}

func (m *module) SetUsageService(usageService any) {
	if service, ok := usageService.(clusterusage.Service); ok {
		m.usageService = service
	}
}

func (m *module) Name() string {
	return Name
}

func (m *module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Usage
}

func (m *module) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	// Usage module configuration
	m.config = params.GetConfig()
	if err := parseUsageConfig(m.config); err != nil {
		return err
	}

	if m.config.Cluster.Hostname == "" {
		return fmt.Errorf("cluster hostname is not set")
	}

	if m.config.Usage.S3Bucket == nil || m.config.Usage.S3Bucket.Get() == "" {
		return fmt.Errorf("S3 bucket name not configured")
	}

	m.policyVersion = DefaultVersion
	if m.config.Usage.PolicyVersion != nil && m.config.Usage.PolicyVersion.Get() != "" {
		m.policyVersion = m.config.Usage.PolicyVersion.Get()
	}

	m.logger = params.GetLogger()
	m.logger = m.logger.WithField("component", Name)
	m.metrics = NewMetrics(params.GetMetricsRegisterer())

	// Configure AWS session
	// Use AWS credentials from environment or IAM role
	awsConfig := aws.NewConfig()

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return fmt.Errorf("failed to create AWS session: %w", err)
	}

	m.s3Client = s3.New(sess)
	m.bucketName = m.config.Usage.S3Bucket.Get()

	m.nodeID = m.config.Cluster.Hostname
	if m.config.Usage.ScrapeInterval != nil {
		if interval := m.config.Usage.ScrapeInterval.Get(); interval > 0 {
			m.interval = interval
		}
	}

	if m.config.Usage.S3Prefix != nil {
		m.prefix = m.config.Usage.S3Prefix.Get()
	}

	m.logger.WithFields(logrus.Fields{
		"node_id":             m.nodeID,
		"collection_interval": m.interval,
		"s3_bucket":           m.config.Usage.S3Bucket.Get(),
		"s3_prefix":           m.config.Usage.S3Prefix.Get(),
	}).Info("initializing usage-s3 module with configuration")

	// Verify bucket permissions by attempting to access the bucket
	if err := m.verifyBucketPermissions(ctx); err != nil {
		return fmt.Errorf("bucket permission check failed: %w", err)
	}

	// Start periodic collection and upload
	enterrors.GoWrapper(func() {
		m.collectAndUploadPeriodically(context.Background())
	}, m.logger)

	m.logger.Info("usage-s3 module initialized successfully")
	return nil
}

// verifyBucketPermissions checks if the module can access the bucket
func (m *module) verifyBucketPermissions(ctx context.Context) error {
	if m.s3Client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	m.logger.WithFields(logrus.Fields{
		"action": "verify_bucket_permissions",
		"bucket": m.bucketName,
		"prefix": m.prefix,
	}).Info("")

	// Skip permission check for local testing environments
	if os.Getenv("CLUSTER_IN_LOCALHOST") != "" {
		m.logger.Info("bucket access verification ignored for localhost")
		return nil
	}

	// Test bucket access by listing objects (limited to 1)
	_, err := m.s3Client.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(m.bucketName),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return fmt.Errorf("S3 permission check failed for bucket %s: %w", m.bucketName, err)
	}

	m.logger.WithFields(logrus.Fields{
		"bucket": m.bucketName,
		"prefix": m.prefix,
	}).Info("S3 permissions verified successfully")

	return nil
}

func (m *module) collectAndUploadPeriodically(ctx context.Context) {
	// Validate intervals before creating tickers
	if m.interval <= 0 {
		m.logger.Warn("Invalid collection interval (<= 0), using default of 1 hour")
		m.interval = DefaultCollectionInterval
	}

	loadInterval := m.config.RuntimeOverrides.LoadInterval
	if loadInterval <= 0 {
		m.logger.Warn("Invalid runtime overrides load interval (<= 0), using default of 2 minutes")
		loadInterval = DefaultRuntimeLoadInterval
	}

	m.logger.WithFields(logrus.Fields{
		"base_interval":  m.interval,
		"load_interval":  loadInterval,
		"default_jitter": DefaultJitterInterval,
	}).Debug("starting periodic collection with ticker")

	// Create ticker with base interval
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	loadTicker := time.NewTicker(loadInterval)
	defer loadTicker.Stop()

	m.logger.WithFields(logrus.Fields{
		"interval":       m.interval,
		"ticker_created": time.Now(),
		"next_fire":      time.Now().Add(m.interval),
	}).Debug("ticker created successfully, entering main loop")

	for {
		select {
		case <-ticker.C:
			m.logger.WithFields(logrus.Fields{
				"current_time": time.Now(),
			}).Debug("ticker fired - starting collection cycle")

			if err := m.collectAndUploadUsage(ctx); err != nil {
				m.logger.WithError(err).Error("Failed to collect and upload usage data")
				m.metrics.OperationTotal.WithLabelValues("collect_and_upload", "error").Inc()
			} else {
				m.metrics.OperationTotal.WithLabelValues("collect_and_upload", "success").Inc()
			}
			// ticker is used to reset the interval
			m.reloadConfig(ticker)

		case <-loadTicker.C:
			m.logger.Debug("runtime overrides reloaded")
			// ticker is used to reset the interval
			m.reloadConfig(ticker)

		case <-ctx.Done():
			m.logger.WithFields(logrus.Fields{"error": ctx.Err()}).Info("context cancelled - stopping periodic collection")
			return
		case <-m.stopChan:
			m.logger.Info("stop signal received - stopping periodic collection")
			return
		}
	}
}

func (m *module) collectAndUploadUsage(ctx context.Context) error {
	// Collect usage data and update metrics
	usage, err := m.collectUsageData(ctx)
	if err != nil {
		return err
	}
	// set version
	usage.Version = m.policyVersion

	// Upload the collected data
	return m.uploadUsageData(ctx, usage)
}

func (m *module) collectUsageData(ctx context.Context) (*types.Report, error) {
	usage, err := m.usageService.Usage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get usage data: %w", err)
	}

	// Compute total collections (from usage.SingleTenantCollections) and update gauge.
	totalCollections := float64(len(usage.Collections))
	m.metrics.ResourceCount.WithLabelValues("collections").Set(totalCollections)

	// Compute total shards (by summing usage.SingleTenantCollections[i].UniqueShardCount) and update gauge.
	var totalShards float64
	for _, coll := range usage.Collections {
		totalShards += float64(coll.UniqueShardCount)
	}
	m.metrics.ResourceCount.WithLabelValues("shards").Set(totalShards)

	// Compute total backups (from usage.Backups) and update gauge.
	totalBackups := float64(len(usage.Backups))
	m.metrics.ResourceCount.WithLabelValues("backups").Set(totalBackups)

	return usage, nil
}

func (m *module) uploadUsageData(ctx context.Context, usage *types.Report) error {
	if m.s3Client == nil {
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

	s3Key := fmt.Sprintf("%s/%s", m.nodeID, filename)
	if m.prefix != "" {
		s3Key = fmt.Sprintf("%s/%s/%s", m.prefix, m.nodeID, filename)
	}

	// Upload to S3
	_, err = m.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(m.bucketName),
		Key:         aws.String(s3Key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
		Metadata: map[string]*string{
			"timestamp": aws.String(timestamp),
			"version":   aws.String(m.policyVersion),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	// Set uploaded file size metric
	m.metrics.UploadedFileSize.Set(float64(len(data)))

	return nil
}

func (m *module) Close() error {
	close(m.stopChan)
	// S3 client doesn't need explicit closing
	return nil
}

func (m *module) Logger() logrus.FieldLogger {
	return m.logger
}

func (m *module) reloadConfig(ticker *time.Ticker) {
	// Check for interval updates
	if interval := m.config.Usage.ScrapeInterval.Get(); interval > 0 && m.interval != interval {
		m.logger.WithFields(logrus.Fields{
			"old_interval": m.interval,
			"new_interval": interval,
		}).Info("collection interval updated")
		m.interval = interval
		// Reset ticker with new interval
		ticker.Reset(m.interval)
	} else if interval <= 0 && m.interval <= 0 {
		// If both old and new intervals are invalid, set a default
		m.logger.Warn("Invalid interval detected during reload, using default of 1 hour")
		m.interval = DefaultCollectionInterval
		ticker.Reset(m.interval)
	}

	// Check for bucket name changes
	if bucketName := m.config.Usage.S3Bucket.Get(); bucketName != "" && m.bucketName != bucketName {
		m.logger.WithFields(logrus.Fields{
			"old_bucket": m.bucketName,
			"new_bucket": bucketName,
		}).Warn("bucket name changed - this may require re-authentication")
		m.bucketName = bucketName
	}

	// Check for prefix changes
	if prefix := m.config.Usage.S3Prefix.Get(); m.prefix != "" && m.prefix != prefix {
		m.logger.WithFields(logrus.Fields{
			"old_prefix": m.prefix,
			"new_prefix": prefix,
		}).Info("upload prefix updated")
		m.prefix = prefix
	}
}

func parseUsageConfig(config *config.Config) error {
	if v := os.Getenv("USAGE_S3_BUCKET"); v != "" {
		config.Usage.S3Bucket = runtime.NewDynamicValue(v)
	}
	if v := os.Getenv("USAGE_S3_PREFIX"); v != "" {
		config.Usage.S3Prefix = runtime.NewDynamicValue(v)
	}
	if v := os.Getenv("USAGE_SCRAPE_INTERVAL"); v != "" {
		duration, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid %s: %w", "USAGE_SCRAPE_INTERVAL", err)
		}
		config.Usage.ScrapeInterval = runtime.NewDynamicValue(duration)
	}
	if v := os.Getenv("USAGE_POLICY_VERSION"); v != "" {
		config.Usage.PolicyVersion = runtime.NewDynamicValue(v)
	}
	return nil
}

// verify we implement the modules.ModuleWithClose interface
var (
	_ = modulecapabilities.ModuleWithClose(New())
	_ = modulecapabilities.ModuleWithUsageService(New())
)
