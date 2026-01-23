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

package backuptest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

// BackupTestSuiteConfig configures the backup test suite.
type BackupTestSuiteConfig struct {
	// BackendType is the backup backend type: "s3", "gcs", or "azure"
	BackendType string

	// BucketName is the bucket/container name for backups
	BucketName string

	// Region is the region for S3 (ignored for other backends)
	Region string

	// ClassName is the test class name
	ClassName string

	// BackupID is the base backup ID
	BackupID string

	// MultiTenant enables multi-tenant testing
	MultiTenant bool

	// NumTenants is the number of tenants for multi-tenant tests
	NumTenants int

	// ObjectsPerTenant is the number of objects to create per tenant (or total if not multi-tenant)
	ObjectsPerTenant int

	// ClusterSize is the number of Weaviate nodes (1 for single-node, 3 for cluster)
	ClusterSize int

	// WithVectorizer enables text2vec-contextionary vectorizer
	WithVectorizer bool

	// TestTimeout is the overall test timeout
	TestTimeout time.Duration

	// BackupTimeout is the timeout for backup operations
	BackupTimeout time.Duration

	// RestoreTimeout is the timeout for restore operations
	RestoreTimeout time.Duration

	// ExternalCompose allows using a pre-existing Docker compose instead of creating a new one.
	// When set, SetupCompose() will use this compose and TeardownCompose() becomes a no-op.
	ExternalCompose *docker.DockerCompose

	// MinioEndpoint is the endpoint for the MinIO server (required when using ExternalCompose with S3).
	MinioEndpoint string
}

// DefaultSuiteConfig returns a default configuration for the backup test suite.
// By default, WithVectorizer is enabled to use text2vec-contextionary for consistency.
func DefaultSuiteConfig() *BackupTestSuiteConfig {
	return &BackupTestSuiteConfig{
		BackendType:      "s3",
		BucketName:       "backups",
		Region:           "us-east-1",
		ClassName:        "BackupTestClass",
		BackupID:         "backup-test",
		MultiTenant:      false,
		NumTenants:       3,
		ObjectsPerTenant: 10,
		ClusterSize:      1,
		WithVectorizer:   true, // Always use text2vec-contextionary for consistency
		TestTimeout:      5 * time.Minute,
		BackupTimeout:    2 * time.Minute,
		RestoreTimeout:   2 * time.Minute,
	}
}

// BackupTestSuite provides a reusable test suite for backup functionality.
type BackupTestSuite struct {
	config    *BackupTestSuiteConfig
	compose   *docker.DockerCompose
	dataGen   *TestDataGenerator
	objectIDs []string
	// objectTenants maps object ID to tenant name (empty string for non-MT)
	objectTenants map[string]string
}

// NewBackupTestSuite creates a new backup test suite with the given configuration.
// Any zero-value fields in config will be filled with defaults.
func NewBackupTestSuite(config *BackupTestSuiteConfig) *BackupTestSuite {
	if config == nil {
		config = DefaultSuiteConfig()
	} else {
		// Apply defaults for any zero-value fields
		defaults := DefaultSuiteConfig()
		if config.BackendType == "" {
			config.BackendType = defaults.BackendType
		}
		if config.BucketName == "" {
			config.BucketName = defaults.BucketName
		}
		if config.Region == "" {
			config.Region = defaults.Region
		}
		if config.ClassName == "" {
			config.ClassName = defaults.ClassName
		}
		if config.BackupID == "" {
			config.BackupID = defaults.BackupID
		}
		if config.NumTenants == 0 {
			config.NumTenants = defaults.NumTenants
		}
		if config.ObjectsPerTenant == 0 {
			config.ObjectsPerTenant = defaults.ObjectsPerTenant
		}
		if config.ClusterSize == 0 {
			config.ClusterSize = defaults.ClusterSize
		}
		if config.TestTimeout == 0 {
			config.TestTimeout = defaults.TestTimeout
		}
		if config.BackupTimeout == 0 {
			config.BackupTimeout = defaults.BackupTimeout
		}
		if config.RestoreTimeout == 0 {
			config.RestoreTimeout = defaults.RestoreTimeout
		}
	}

	// Determine vectorizer for test data
	vectorizer := ""
	if config.WithVectorizer {
		vectorizer = "text2vec-contextionary"
	}

	dataGen := NewTestDataGenerator(&TestDataConfig{
		ClassName:        config.ClassName,
		MultiTenant:      config.MultiTenant,
		NumTenants:       config.NumTenants,
		ObjectsPerTenant: config.ObjectsPerTenant,
		UseVectorizer:    vectorizer,
	})

	return &BackupTestSuite{
		config:  config,
		dataGen: dataGen,
	}
}

// SetupCompose starts the Docker compose environment with the appropriate backend.
// If ExternalCompose is set in the config, it uses that instead of creating a new one.
func (s *BackupTestSuite) SetupCompose(ctx context.Context) error {
	// If an external compose is provided, use it instead of creating a new one
	if s.config.ExternalCompose != nil {
		s.compose = s.config.ExternalCompose

		// For S3, ensure the bucket exists
		if s.config.BackendType == "s3" {
			if err := s.ensureS3BucketExists(ctx); err != nil {
				return fmt.Errorf("failed to ensure S3 bucket exists: %w", err)
			}
		}

		// Setup the helper client
		helper.SetupClient(s.compose.GetWeaviate().URI())
		return nil
	}

	// Create a new compose
	compose := docker.New()

	switch s.config.BackendType {
	case "s3":
		compose = compose.WithBackendS3(s.config.BucketName, s.config.Region)
		// S3 module requires AWS_REGION to be set
		if s.config.Region != "" {
			compose = compose.WithWeaviateEnv("AWS_REGION", s.config.Region)
		}
	case "gcs":
		compose = compose.WithBackendGCS(s.config.BucketName)
	case "azure":
		compose = compose.WithBackendAzure(s.config.BucketName)
	default:
		return fmt.Errorf("unsupported backend type: %s", s.config.BackendType)
	}

	// Add vectorizer if configured
	if s.config.WithVectorizer {
		compose = compose.WithText2VecContextionary()
	}

	// Start cluster or single node
	var err error
	if s.config.ClusterSize > 1 {
		s.compose, err = compose.WithWeaviateCluster(s.config.ClusterSize).Start(ctx)
	} else {
		s.compose, err = compose.WithWeaviate().Start(ctx)
	}
	if err != nil {
		return fmt.Errorf("failed to start compose: %w", err)
	}

	// For S3, ensure the bucket exists (the mc command in MinIO may fail silently)
	if s.config.BackendType == "s3" {
		if err := s.ensureS3BucketExists(ctx); err != nil {
			return fmt.Errorf("failed to ensure S3 bucket exists: %w", err)
		}
	}

	// Setup the helper client
	helper.SetupClient(s.compose.GetWeaviate().URI())

	return nil
}

// ensureS3BucketExists creates the S3 bucket if it doesn't exist.
func (s *BackupTestSuite) ensureS3BucketExists(ctx context.Context) error {
	// Determine the MinIO endpoint
	var minioEndpoint string
	if s.config.MinioEndpoint != "" {
		minioEndpoint = s.config.MinioEndpoint
	} else if s.compose != nil && s.compose.GetMinIO() != nil {
		minioEndpoint = s.compose.GetMinIO().URI()
	} else {
		return fmt.Errorf("no MinIO endpoint available")
	}

	// Create AWS config for MinIO
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s.config.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"aws_access_key", // MinIO default access key
			"aws_secret_key", // MinIO default secret key
			"",
		)),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client pointing to MinIO
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + minioEndpoint)
		o.UsePathStyle = true // Required for MinIO
	})

	// Check if bucket exists
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.BucketName),
	})
	if err == nil {
		// Bucket already exists
		return nil
	}

	// Create the bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.config.BucketName),
	})
	if err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", s.config.BucketName, err)
	}

	return nil
}

// TeardownCompose stops the Docker compose environment.
// If using an external compose (set via ExternalCompose config), this is a no-op
// since the external compose lifecycle is managed by the caller.
func (s *BackupTestSuite) TeardownCompose(ctx context.Context) error {
	// If using external compose, don't terminate it (managed externally)
	if s.config.ExternalCompose != nil {
		return nil
	}

	if s.compose != nil {
		return s.compose.Terminate(ctx)
	}
	return nil
}

// CreateTestClass creates the test class schema.
func (s *BackupTestSuite) CreateTestClass(t *testing.T) {
	t.Helper()
	class := s.dataGen.GenerateClass()
	helper.CreateClass(t, class)
}

// DeleteTestClass removes the test class.
func (s *BackupTestSuite) DeleteTestClass(t *testing.T) {
	t.Helper()
	helper.DeleteClass(t, s.config.ClassName)
}

// CreateTestTenants creates tenants for multi-tenant tests.
func (s *BackupTestSuite) CreateTestTenants(t *testing.T) {
	t.Helper()
	if !s.config.MultiTenant {
		return
	}

	tenants := s.dataGen.GenerateTenantModels()
	helper.CreateTenants(t, s.config.ClassName, tenants)
}

// CreateTestObjects creates test objects and stores their IDs.
func (s *BackupTestSuite) CreateTestObjects(t *testing.T) {
	t.Helper()

	objects := s.dataGen.GenerateAllObjects()
	s.objectIDs = make([]string, len(objects))
	s.objectTenants = make(map[string]string, len(objects))

	for i, obj := range objects {
		id := obj.ID.String()
		s.objectIDs[i] = id
		s.objectTenants[id] = obj.Tenant // Empty string for non-MT
	}

	// Batch create objects
	helper.CreateObjectsBatch(t, objects)
}

// VerifyObjectsExist checks that all created objects still exist using aggregate count.
// This is much faster than checking individual objects.
func (s *BackupTestSuite) VerifyObjectsExist(t *testing.T) {
	t.Helper()

	expectedCount := int64(len(s.objectIDs))

	if s.config.MultiTenant {
		// For multi-tenant, check count per tenant
		tenants := s.dataGen.GenerateTenants()
		expectedPerTenant := int64(s.config.ObjectsPerTenant)
		for _, tenant := range tenants {
			count := moduleshelper.GetClassCount(t, s.config.ClassName, tenant)
			require.Equal(t, expectedPerTenant, count,
				"tenant %s should have %d objects, got %d", tenant, expectedPerTenant, count)
		}
	} else {
		// For single-tenant, check total count
		count := moduleshelper.GetClassCount(t, s.config.ClassName, "")
		require.Equal(t, expectedCount, count,
			"class should have %d objects, got %d", expectedCount, count)
	}
}

// VerifyObjectsDoNotExist checks that objects no longer exist (after class deletion).
// Uses class existence check since the class should be deleted.
func (s *BackupTestSuite) VerifyObjectsDoNotExist(t *testing.T) {
	t.Helper()

	// After class deletion, trying to get the class should return an error
	_, err := helper.GetClassWithoutAssert(t, s.config.ClassName)
	require.Error(t, err, "class %s should not exist after deletion", s.config.ClassName)
}

// CreateBackup creates a backup and waits for it to complete.
func (s *BackupTestSuite) CreateBackup(t *testing.T, backupID string) {
	t.Helper()

	cfg := helper.DefaultBackupConfig()

	// Start backup
	resp, err := helper.CreateBackup(t, cfg, s.config.ClassName, s.config.BackendType, backupID)
	if err != nil {
		// Try to extract detailed error message from the response
		t.Logf("Backup creation failed with error type: %T", err)

		// Check for unprocessable entity error
		var uerr *backups.BackupsCreateUnprocessableEntity
		if errors.As(err, &uerr) && uerr.Payload != nil {
			for i, e := range uerr.Payload.Error {
				t.Logf("Error[%d]: %s", i, e.Message)
			}
		}
	}
	require.NoError(t, err, "create backup should succeed")
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	assert.Equal(t, backupID, resp.Payload.ID)

	// Wait for backup to complete
	helper.ExpectBackupEventuallyCreated(t, backupID, s.config.BackendType, nil,
		helper.WithDeadline(s.config.BackupTimeout))
}

// RestoreBackup restores a backup and waits for it to complete.
func (s *BackupTestSuite) RestoreBackup(t *testing.T, backupID string) {
	t.Helper()

	cfg := helper.DefaultRestoreConfig()

	// Start restore
	resp, err := helper.RestoreBackup(t, cfg, s.config.ClassName, s.config.BackendType, backupID, nil, false)
	require.NoError(t, err, "restore backup should succeed")
	require.NotNil(t, resp)
	require.NotNil(t, resp.Payload)
	assert.Equal(t, backupID, resp.Payload.ID)

	// Wait for restore to complete
	helper.ExpectBackupEventuallyRestored(t, backupID, s.config.BackendType, nil,
		helper.WithDeadline(s.config.RestoreTimeout))
}

// RunBasicBackupRestoreTest runs a complete backup and restore test cycle.
func (s *BackupTestSuite) RunBasicBackupRestoreTest(t *testing.T) {
	backupID := fmt.Sprintf("%s-%d", s.config.BackupID, time.Now().UnixNano())

	t.Run("create class", func(t *testing.T) {
		s.CreateTestClass(t)
	})

	if s.config.MultiTenant {
		t.Run("create tenants", func(t *testing.T) {
			s.CreateTestTenants(t)
		})
	}

	t.Run("create objects", func(t *testing.T) {
		s.CreateTestObjects(t)
	})

	t.Run("verify objects exist", func(t *testing.T) {
		s.VerifyObjectsExist(t)
	})

	t.Run("create backup", func(t *testing.T) {
		s.CreateBackup(t, backupID)
	})

	t.Run("delete class", func(t *testing.T) {
		s.DeleteTestClass(t)
		time.Sleep(time.Second) // Wait for deletion to propagate
	})

	t.Run("verify objects do not exist", func(t *testing.T) {
		s.VerifyObjectsDoNotExist(t)
	})

	t.Run("restore backup", func(t *testing.T) {
		s.RestoreBackup(t, backupID)
	})

	t.Run("verify objects restored", func(t *testing.T) {
		s.VerifyObjectsExist(t)
	})

	t.Run("cleanup", func(t *testing.T) {
		s.DeleteTestClass(t)
	})
}

// RunAllTests runs all backup test scenarios.
// This method manages its own compose lifecycle unless ExternalCompose is set.
func (s *BackupTestSuite) RunAllTests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.TestTimeout)
	defer cancel()

	// Setup
	require.NoError(t, s.SetupCompose(ctx), "setup compose")
	defer func() {
		if err := s.TeardownCompose(ctx); err != nil {
			t.Logf("Warning: teardown failed: %v", err)
		}
	}()

	// Run tests
	t.Run("basic_backup_restore", func(t *testing.T) {
		s.RunBasicBackupRestoreTest(t)
	})
}

// RunTestsWithSharedCompose runs all backup test scenarios using a pre-existing compose.
// This is optimized for test suites that share a single cluster across multiple tests.
// The compose lifecycle is NOT managed by this method - caller is responsible for setup/teardown.
func (s *BackupTestSuite) RunTestsWithSharedCompose(t *testing.T, compose *docker.DockerCompose) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.TestTimeout)
	defer cancel()

	// Set the compose
	s.compose = compose

	// For S3, ensure the bucket exists
	if s.config.BackendType == "s3" {
		require.NoError(t, s.ensureS3BucketExists(ctx), "ensure S3 bucket exists")
	}

	// Run tests
	t.Run("basic_backup_restore", func(t *testing.T) {
		s.RunBasicBackupRestoreTest(t)
	})
}

// BackendTestCase defines a test case for a specific backend.
type BackendTestCase struct {
	Name        string
	BackendType string
	BucketName  string
	Region      string // Only for S3
}

// StandardBackendTestCases returns test cases for all standard backends.
func StandardBackendTestCases() []BackendTestCase {
	return []BackendTestCase{
		{
			Name:        "S3",
			BackendType: "s3",
			BucketName:  "backups",
			Region:      "us-east-1",
		},
		{
			Name:        "GCS",
			BackendType: "gcs",
			BucketName:  "backups",
		},
		{
			Name:        "Azure",
			BackendType: "azure",
			BucketName:  "backups",
		},
	}
}

// AssertBackupSucceeded is a helper to verify a backup completed successfully.
func AssertBackupSucceeded(t *testing.T, backend, backupID string) {
	t.Helper()
	resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
	require.NoError(t, err)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Status)
	assert.Equal(t, "SUCCESS", *resp.Payload.Status, "backup should succeed")
}

// AssertRestoreSucceeded is a helper to verify a restore completed successfully.
func AssertRestoreSucceeded(t *testing.T, backend, backupID string) {
	t.Helper()
	resp, err := helper.RestoreBackupStatus(t, backend, backupID, "", "")
	require.NoError(t, err)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Status)
	assert.Equal(t, "SUCCESS", *resp.Payload.Status, "restore should succeed")
}

// GetObjectCount returns the number of objects created by this test suite.
func (s *BackupTestSuite) GetObjectCount() int {
	return len(s.objectIDs)
}

// GetObjectIDs returns the IDs of all objects created by this test suite.
func (s *BackupTestSuite) GetObjectIDs() []string {
	return s.objectIDs
}

// UniqueTestID generates a unique identifier for test isolation.
// Returns just the timestamp portion - callers should format appropriately:
// - Class names: use underscores (dashes invalid in Weaviate)
// - Bucket names: use dashes (underscores invalid in S3)
func UniqueTestID(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
}

// UniqueBucketName generates a valid S3 bucket name (lowercase, dashes allowed, no underscores).
func UniqueBucketName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// UniqueClassName generates a valid Weaviate class name (underscores allowed, no dashes).
func UniqueClassName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// BackupTestCase defines a test case configuration that can be extended with new options.
// This allows adding new test parameters without changing function signatures.
type BackupTestCase struct {
	// Name is a short identifier for the test case (used in class/backup naming)
	Name string

	// MultiTenant enables multi-tenant testing
	MultiTenant bool

	// NumTenants overrides the default number of tenants (default: 3, or 50 for multi-tenant)
	NumTenants int

	// ObjectsPerTenant overrides the default objects per tenant (default: 10)
	ObjectsPerTenant int

	// Future extensibility: add new test case options here without breaking existing code
	// Examples: compression settings, async backup, node failure scenarios, etc.
}

// DefaultTestCase returns a test case with default settings (single-tenant).
func DefaultTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "default",
		MultiTenant:      false,
		NumTenants:       3,
		ObjectsPerTenant: 10,
	}
}

// SingleTenantTestCase returns a test case for single-tenant backup testing.
func SingleTenantTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "single_tenant",
		MultiTenant:      false,
		ObjectsPerTenant: 10,
	}
}

// MultiTenantTestCase returns a test case for multi-tenant backup testing.
func MultiTenantTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "multi_tenant",
		MultiTenant:      true,
		NumTenants:       50,
		ObjectsPerTenant: 10,
	}
}

// SharedComposeConfig holds configuration for the shared Docker compose environment.
type SharedComposeConfig struct {
	// Compose is the shared Docker compose instance
	Compose *docker.DockerCompose
	// MinioEndpoint is the MinIO endpoint (for S3 tests)
	MinioEndpoint string
	// Region is the S3 region
	Region string
}

// NewSuiteConfigFromTestCase creates a BackupTestSuiteConfig from a test case and shared compose.
// It generates unique class names and backup IDs for test isolation.
func NewSuiteConfigFromTestCase(sharedConfig SharedComposeConfig, testCase BackupTestCase) *BackupTestSuiteConfig {
	timestamp := time.Now().UnixNano()

	// Determine number of tenants
	numTenants := testCase.NumTenants
	if testCase.MultiTenant && numTenants == 0 {
		numTenants = 50 // Default for multi-tenant when not specified
	}

	// Determine objects per tenant
	objectsPerTenant := testCase.ObjectsPerTenant
	if objectsPerTenant == 0 {
		objectsPerTenant = 10
	}

	return &BackupTestSuiteConfig{
		BackendType:      "s3",
		BucketName:       "backups", // Use the default bucket created by shared cluster
		Region:           sharedConfig.Region,
		ClassName:        fmt.Sprintf("Class_%s_%d", testCase.Name, timestamp),
		BackupID:         fmt.Sprintf("backup-%s-%d", testCase.Name, timestamp),
		MultiTenant:      testCase.MultiTenant,
		NumTenants:       numTenants,
		ObjectsPerTenant: objectsPerTenant,
		ClusterSize:      3, // Assume cluster for shared compose
		WithVectorizer:   true,
		TestTimeout:      5 * time.Minute,
		BackupTimeout:    2 * time.Minute,
		RestoreTimeout:   2 * time.Minute,
		ExternalCompose:  sharedConfig.Compose,
		MinioEndpoint:    sharedConfig.MinioEndpoint,
	}
}

// RunS3BackupTests runs S3 backup tests using a shared compose and test case configuration.
func RunS3BackupTests(t *testing.T, compose *docker.DockerCompose, minioEndpoint, region string, testCase BackupTestCase) {
	sharedConfig := SharedComposeConfig{
		Compose:       compose,
		MinioEndpoint: minioEndpoint,
		Region:        region,
	}

	config := NewSuiteConfigFromTestCase(sharedConfig, testCase)
	suite := NewBackupTestSuite(config)
	suite.RunTestsWithSharedCompose(t, compose)
}
