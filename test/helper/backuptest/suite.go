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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
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

	// TestTimeout is the overall test timeout
	TestTimeout time.Duration

	// BackupTimeout is the timeout for backup operations
	BackupTimeout time.Duration

	// RestoreTimeout is the timeout for restore operations
	RestoreTimeout time.Duration
}

// DefaultSuiteConfig returns a default configuration for the backup test suite.
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
}

// NewBackupTestSuite creates a new backup test suite with the given configuration.
func NewBackupTestSuite(config *BackupTestSuiteConfig) *BackupTestSuite {
	if config == nil {
		config = DefaultSuiteConfig()
	}

	dataGen := NewTestDataGenerator(&TestDataConfig{
		ClassName:        config.ClassName,
		MultiTenant:      config.MultiTenant,
		NumTenants:       config.NumTenants,
		ObjectsPerTenant: config.ObjectsPerTenant,
	})

	return &BackupTestSuite{
		config:  config,
		dataGen: dataGen,
	}
}

// SetupCompose starts the Docker compose environment with the appropriate backend.
func (s *BackupTestSuite) SetupCompose(ctx context.Context) error {
	compose := docker.New()

	switch s.config.BackendType {
	case "s3":
		compose = compose.WithBackendS3(s.config.BucketName, s.config.Region)
	case "gcs":
		compose = compose.WithBackendGCS(s.config.BucketName)
	case "azure":
		compose = compose.WithBackendAzure(s.config.BucketName)
	default:
		return fmt.Errorf("unsupported backend type: %s", s.config.BackendType)
	}

	var err error
	s.compose, err = compose.WithWeaviate().Start(ctx)
	if err != nil {
		return fmt.Errorf("failed to start compose: %w", err)
	}

	// Setup the helper client
	helper.SetupClient(s.compose.GetWeaviate().URI())

	return nil
}

// TeardownCompose stops the Docker compose environment.
func (s *BackupTestSuite) TeardownCompose(ctx context.Context) error {
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

	for i, obj := range objects {
		s.objectIDs[i] = obj.ID.String()
	}

	// Batch create objects
	helper.CreateObjectsBatch(t, objects)
}

// VerifyObjectsExist checks that all created objects still exist.
func (s *BackupTestSuite) VerifyObjectsExist(t *testing.T) {
	t.Helper()

	for _, id := range s.objectIDs {
		obj := helper.AssertGetObject(t, s.config.ClassName, strfmt.UUID(id))
		require.NotNil(t, obj, "object %s should exist", id)
	}
}

// VerifyObjectsDoNotExist checks that objects no longer exist (after class deletion).
func (s *BackupTestSuite) VerifyObjectsDoNotExist(t *testing.T) {
	t.Helper()

	for _, id := range s.objectIDs {
		err := helper.AssertGetObjectFailsEventually(t, s.config.ClassName, strfmt.UUID(id))
		require.NotNil(t, err, "object %s should not exist", id)
	}
}

// CreateBackup creates a backup and waits for it to complete.
func (s *BackupTestSuite) CreateBackup(t *testing.T, backupID string) {
	t.Helper()

	cfg := helper.DefaultBackupConfig()

	// Start backup
	resp, err := helper.CreateBackup(t, cfg, s.config.ClassName, s.config.BackendType, backupID)
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

// RunTableDrivenBackupTests runs backup tests against multiple backends.
func RunTableDrivenBackupTests(t *testing.T, testCases []BackendTestCase) {
	for _, tc := range testCases {
		tc := tc // Capture range variable
		t.Run(tc.Name, func(t *testing.T) {
			config := DefaultSuiteConfig()
			config.BackendType = tc.BackendType
			config.BucketName = tc.BucketName
			config.Region = tc.Region

			suite := NewBackupTestSuite(config)
			suite.RunAllTests(t)
		})
	}
}

// RunS3BackupTests runs backup tests specifically for S3.
func RunS3BackupTests(t *testing.T, multiTenant bool) {
	config := DefaultSuiteConfig()
	config.BackendType = "s3"
	config.MultiTenant = multiTenant
	if multiTenant {
		config.ClassName = "BackupTestClassMT"
		config.BackupID = "backup-test-mt"
	}

	suite := NewBackupTestSuite(config)
	suite.RunAllTests(t)
}

// RunGCSBackupTests runs backup tests specifically for GCS.
func RunGCSBackupTests(t *testing.T, multiTenant bool) {
	config := DefaultSuiteConfig()
	config.BackendType = "gcs"
	config.MultiTenant = multiTenant
	if multiTenant {
		config.ClassName = "BackupTestClassMT"
		config.BackupID = "backup-test-mt"
	}

	suite := NewBackupTestSuite(config)
	suite.RunAllTests(t)
}

// RunAzureBackupTests runs backup tests specifically for Azure.
func RunAzureBackupTests(t *testing.T, multiTenant bool) {
	config := DefaultSuiteConfig()
	config.BackendType = "azure"
	config.MultiTenant = multiTenant
	if multiTenant {
		config.ClassName = "BackupTestClassMT"
		config.BackupID = "backup-test-mt"
	}

	suite := NewBackupTestSuite(config)
	suite.RunAllTests(t)
}

// QuickSuiteConfig returns a configuration optimized for quick validation tests.
func QuickSuiteConfig() *BackupTestSuiteConfig {
	return &BackupTestSuiteConfig{
		BackendType:      "s3",
		BucketName:       "backups",
		Region:           "us-east-1",
		ClassName:        "QuickBackupTest",
		BackupID:         "quick-backup",
		MultiTenant:      false,
		NumTenants:       1,
		ObjectsPerTenant: 3, // Minimal objects for quick tests
		TestTimeout:      3 * time.Minute,
		BackupTimeout:    1 * time.Minute,
		RestoreTimeout:   1 * time.Minute,
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
