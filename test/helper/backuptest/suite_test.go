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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackupTestSuiteConfig_Defaults(t *testing.T) {
	config := DefaultSuiteConfig()

	assert.Equal(t, "s3", config.BackendType)
	assert.Equal(t, "backups", config.BucketName)
	assert.Equal(t, "us-east-1", config.Region)
	assert.Equal(t, "BackupTestClass", config.ClassName)
	assert.Equal(t, "backup-test", config.BackupID)
	assert.False(t, config.MultiTenant)
	assert.Equal(t, 3, config.NumTenants)
	assert.Equal(t, 10, config.ObjectsPerTenant)
	assert.Equal(t, 5*time.Minute, config.TestTimeout)
	assert.Equal(t, 2*time.Minute, config.BackupTimeout)
	assert.Equal(t, 2*time.Minute, config.RestoreTimeout)
}

func TestBackupTestSuite_Creation(t *testing.T) {
	t.Run("with nil config uses defaults", func(t *testing.T) {
		suite := NewBackupTestSuite(nil)
		require.NotNil(t, suite)
		assert.Equal(t, "s3", suite.config.BackendType)
		assert.NotNil(t, suite.dataGen)
	})

	t.Run("with custom config", func(t *testing.T) {
		config := &BackupTestSuiteConfig{
			BackendType:      "gcs",
			BucketName:       "custom-bucket",
			ClassName:        "CustomClass",
			BackupID:         "custom-backup",
			MultiTenant:      true,
			NumTenants:       5,
			ObjectsPerTenant: 20,
		}
		suite := NewBackupTestSuite(config)

		require.NotNil(t, suite)
		assert.Equal(t, "gcs", suite.config.BackendType)
		assert.Equal(t, "custom-bucket", suite.config.BucketName)
		assert.True(t, suite.config.MultiTenant)
	})
}

func TestBackupTestSuite_DataGeneration(t *testing.T) {
	config := &BackupTestSuiteConfig{
		ClassName:        "TestClass",
		MultiTenant:      true,
		NumTenants:       3,
		ObjectsPerTenant: 5,
	}
	suite := NewBackupTestSuite(config)

	// Verify data generator is configured correctly
	class := suite.dataGen.GenerateClass()
	assert.Equal(t, "TestClass", class.Class)
	assert.NotNil(t, class.MultiTenancyConfig)
	assert.True(t, class.MultiTenancyConfig.Enabled)

	// Verify tenant generation
	tenants := suite.dataGen.GenerateTenants()
	assert.Len(t, tenants, 3)

	// Verify object generation
	objects := suite.dataGen.GenerateAllObjects()
	assert.Len(t, objects, 15) // 3 tenants * 5 objects
}

func TestStandardBackendTestCases(t *testing.T) {
	cases := StandardBackendTestCases()

	require.Len(t, cases, 3)

	// Verify S3 case
	assert.Equal(t, "S3", cases[0].Name)
	assert.Equal(t, "s3", cases[0].BackendType)
	assert.NotEmpty(t, cases[0].Region)

	// Verify GCS case
	assert.Equal(t, "GCS", cases[1].Name)
	assert.Equal(t, "gcs", cases[1].BackendType)

	// Verify Azure case
	assert.Equal(t, "Azure", cases[2].Name)
	assert.Equal(t, "azure", cases[2].BackendType)
}

func TestBackupTestSuite_ObjectIDs(t *testing.T) {
	suite := NewBackupTestSuite(&BackupTestSuiteConfig{
		ClassName:        "TestClass",
		ObjectsPerTenant: 5,
	})

	// Before creating objects, counts should be zero
	assert.Equal(t, 0, suite.GetObjectCount())
	assert.Empty(t, suite.GetObjectIDs())
}

// TestBackupTestSuite_Integration is an integration test that requires Docker.
// It's skipped by default and should be run explicitly.
func TestBackupTestSuite_S3Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This test would normally run the full suite against S3
	// For validation purposes, we just verify the suite can be created
	// Full integration tests will be added in the refactoring phase

	t.Run("suite creation for S3", func(t *testing.T) {
		suite := NewBackupTestSuite(&BackupTestSuiteConfig{
			BackendType:      "s3",
			BucketName:       "backups",
			Region:           "us-east-1",
			ClassName:        "IntegrationTestClass",
			ObjectsPerTenant: 3,
		})
		require.NotNil(t, suite)
		assert.Equal(t, "s3", suite.config.BackendType)
	})

	t.Run("suite creation for GCS", func(t *testing.T) {
		suite := NewBackupTestSuite(&BackupTestSuiteConfig{
			BackendType:      "gcs",
			BucketName:       "backups",
			ClassName:        "IntegrationTestClass",
			ObjectsPerTenant: 3,
		})
		require.NotNil(t, suite)
		assert.Equal(t, "gcs", suite.config.BackendType)
	})

	t.Run("suite creation for Azure", func(t *testing.T) {
		suite := NewBackupTestSuite(&BackupTestSuiteConfig{
			BackendType:      "azure",
			BucketName:       "backups",
			ClassName:        "IntegrationTestClass",
			ObjectsPerTenant: 3,
		})
		require.NotNil(t, suite)
		assert.Equal(t, "azure", suite.config.BackendType)
	})
}

func TestBackupTestSuite_ConfigVariants(t *testing.T) {
	t.Run("single tenant - no multi-tenancy", func(t *testing.T) {
		// Explicit single-tenant configuration - no multi-tenancy enabled
		config := &BackupTestSuiteConfig{
			BackendType:      "s3",
			BucketName:       "backups",
			ClassName:        "SingleTenantClass",
			BackupID:         "single-tenant-backup",
			MultiTenant:      false, // Explicitly disabled
			ObjectsPerTenant: 10,    // Total objects (no tenants)
		}
		suite := NewBackupTestSuite(config)

		// Class should NOT have multi-tenancy config
		class := suite.dataGen.GenerateClass()
		assert.Nil(t, class.MultiTenancyConfig, "single tenant class should not have MultiTenancyConfig")

		// All objects should have no tenant
		objects := suite.dataGen.GenerateAllObjects()
		assert.Len(t, objects, 10, "should create exactly ObjectsPerTenant objects")

		// Verify no objects have a tenant set
		for _, obj := range objects {
			assert.Empty(t, obj.Tenant, "single tenant objects should not have Tenant set")
		}
	})

	t.Run("multi tenant - with multiple tenants", func(t *testing.T) {
		// Explicit multi-tenant configuration
		config := &BackupTestSuiteConfig{
			BackendType:      "s3",
			BucketName:       "backups",
			ClassName:        "MultiTenantClass",
			BackupID:         "multi-tenant-backup",
			MultiTenant:      true, // Explicitly enabled
			NumTenants:       4,    // 4 distinct tenants
			ObjectsPerTenant: 5,    // 5 objects per tenant
		}
		suite := NewBackupTestSuite(config)

		// Class SHOULD have multi-tenancy config enabled
		class := suite.dataGen.GenerateClass()
		require.NotNil(t, class.MultiTenancyConfig, "multi tenant class should have MultiTenancyConfig")
		assert.True(t, class.MultiTenancyConfig.Enabled, "multi-tenancy should be enabled")

		// Total objects = NumTenants * ObjectsPerTenant
		objects := suite.dataGen.GenerateAllObjects()
		assert.Len(t, objects, 20, "should create NumTenants * ObjectsPerTenant objects (4 * 5 = 20)")

		// Verify tenant distribution - each tenant should have exactly ObjectsPerTenant objects
		byTenant := suite.dataGen.ObjectsByTenant(objects)
		assert.Len(t, byTenant, 4, "should have exactly 4 tenants")
		for tenant, objs := range byTenant {
			assert.NotEmpty(t, tenant, "tenant name should not be empty")
			assert.Len(t, objs, 5, "each tenant should have exactly 5 objects")
		}

		// Verify all objects have a tenant set
		for _, obj := range objects {
			assert.NotEmpty(t, obj.Tenant, "multi-tenant objects should have Tenant set")
		}
	})
}

func TestRunBackupTestHelpers(t *testing.T) {
	// Verify helper function signatures compile correctly
	// These functions will be used in actual integration tests

	t.Run("RunS3BackupTests signature", func(t *testing.T) {
		// Just verify the function exists and compiles
		_ = RunS3BackupTests
	})
}
