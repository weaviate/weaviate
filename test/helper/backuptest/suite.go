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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/minio/minio-go/v7"
	minioCredentials "github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/backups"
	gql "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

// CompressionType specifies the vector compression algorithm to use.
type CompressionType string

const (
	// CompressionNone indicates no compression
	CompressionNone CompressionType = ""
	// CompressionPQ indicates Product Quantization compression
	CompressionPQ CompressionType = "pq"
	// CompressionRQ indicates Rotational Quantization compression
	CompressionRQ CompressionType = "rq"
)

// BackupTestSuiteConfig configures the backup test suite.
type BackupTestSuiteConfig struct {
	// BackendType is the backup backend type: "s3", "gcs", "azure", or "filesystem"
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

	// WithCompression specifies the compression algorithm to enable after class creation.
	// Use CompressionPQ for Product Quantization or CompressionRQ for Rotational Quantization.
	WithCompression CompressionType

	// TestCancellation runs backup cancellation test instead of full backup/restore
	TestCancellation bool

	TestIncremental bool
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
	case "filesystem":
		compose = compose.WithBackendFilesystem()
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

// VerifyCompressedVectorsRestored samples random objects and verifies their vectors are present.
// This is used to validate that compressed vectors are correctly restored after backup/restore.
func (s *BackupTestSuite) VerifyCompressedVectorsRestored(t *testing.T, sampleSize int) {
	t.Helper()

	if sampleSize > len(s.objectIDs) {
		sampleSize = len(s.objectIDs)
	}
	if sampleSize == 0 {
		t.Skip("no objects to sample")
		return
	}

	// Sample random object IDs
	sampled := make([]string, sampleSize)
	copy(sampled, s.objectIDs[:sampleSize])

	for _, id := range sampled {
		tenant := s.objectTenants[id]

		// Build GraphQL query to get object with vector
		var query string
		if tenant != "" {
			query = fmt.Sprintf(`{Get{%s(where:{path:["id"],operator:Equal,valueText:"%s"},tenant:%q){_additional{id vector}}}}`,
				s.config.ClassName, id, tenant)
		} else {
			query = fmt.Sprintf(`{Get{%s(where:{path:["id"],operator:Equal,valueText:"%s"}){_additional{id vector}}}}`,
				s.config.ClassName, id)
		}

		resp := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		result := resp.Get("Get", s.config.ClassName).Result.([]interface{})
		require.Len(t, result, 1, "expected 1 object for id %s", id)

		obj := result[0].(map[string]interface{})
		additional := obj["_additional"].(map[string]interface{})

		// Verify vector is present and non-empty
		vector, ok := additional["vector"]
		require.True(t, ok, "object %s should have vector in _additional", id)
		require.NotNil(t, vector, "vector should not be nil for object %s", id)

		vectorSlice, ok := vector.([]interface{})
		require.True(t, ok, "vector should be a slice for object %s", id)
		require.Greater(t, len(vectorSlice), 0, "vector should not be empty for object %s", id)

		t.Logf("Verified object %s has vector with %d dimensions", id, len(vectorSlice))
	}
}

// CreateBackup creates a backup and waits for it to complete.
func (s *BackupTestSuite) CreateBackup(t *testing.T, backupID, baseBackupId string) {
	t.Helper()

	cfg := helper.DefaultBackupConfig()

	// Start backup
	resp, err := helper.CreateBackupWithBase(t, cfg, s.config.ClassName, s.config.BackendType, backupID, baseBackupId)
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

// EnablePQ enables Product Quantization compression on the class.
func (s *BackupTestSuite) EnablePQ(t *testing.T) {
	t.Helper()
	pq := map[string]interface{}{
		"enabled":       true,
		"trainingLimit": 10,
		"segments":      0, // Auto-calculate
	}
	helper.EnablePQ(t, s.config.ClassName, pq)
}

// EnableRQ enables Rotational Quantization compression on the class.
func (s *BackupTestSuite) EnableRQ(t *testing.T) {
	t.Helper()
	class := helper.GetClass(t, s.config.ClassName)
	cfg := class.VectorIndexConfig.(map[string]interface{})
	cfg["rq"] = map[string]interface{}{
		"enabled":       true,
		"trainingLimit": 10,
	}
	class.VectorIndexConfig = cfg
	helper.UpdateClass(t, class)
	// Time for compression to complete
	time.Sleep(2 * time.Second)
}

type book struct {
	Title   string   `fake:"{sentence:10}"`
	Content string   `fake:"{paragraph:10}"`
	Tags    []string `fake:"{words:3}"`
}

func (b *book) toObject(class string) *models.Object {
	return &models.Object{
		Class: class,
		Properties: map[string]interface{}{
			"title":   b.Title,
			"content": b.Content,
			"tags":    b.Tags,
		},
	}
}

func (s *BackupTestSuite) RunIncrementalTestAndRestore(t *testing.T) {
	t.Run("incremental backup and restore errors", s.RunIncrementalTestAndRestoreErrors)
	t.Run("successful incremental backup and restore", s.RunIncrementalTestAndRestoreSuccess)
}

func (s *BackupTestSuite) RunIncrementalTestAndRestoreErrors(t *testing.T) {
	t.Run("base backup does not exist", func(t *testing.T) {
		s.CreateTestClass(t)
		s.CreateTestObjects(t)
		cfg := helper.DefaultBackupConfig()

		_, err := helper.CreateBackupWithBase(t, cfg, s.config.ClassName, s.config.BackendType, "incremental-no-base-"+s.config.BackupID, "non-existent-base-backup"+s.config.BackupID)
		require.Error(t, err)
	})
}

func (s *BackupTestSuite) RunIncrementalTestAndRestoreSuccess(t *testing.T) {
	backupIDBase := fmt.Sprintf("base-%s-%d", s.config.BackupID, time.Now().UnixNano())
	backupIDIncremental1 := fmt.Sprintf("incremental1-%s-%d", s.config.BackupID, time.Now().UnixNano())
	backupIDIncremental2 := fmt.Sprintf("incremental2-%s-%d", s.config.BackupID, time.Now().UnixNano())
	var endpoints []string
	for i := 1; i <= s.config.ClusterSize; i++ {
		endpoints = append(endpoints, s.compose.GetWeaviateNode(i).URI())
	}
	s.DeleteTestClass(t)
	defer s.DeleteTestClass(t)
	s.CreateTestClass(t)

	// add lots of data
	numObjects := 10000
	for i := 0; i < numObjects; i++ {
		var b book
		require.NoError(t, gofakeit.Struct(&b))
		require.NoError(t, helper.CreateObject(t, b.toObject(s.config.ClassName)))
	}
	checkCount(t, endpoints, s.config.ClassName, numObjects)
	s.CreateBackup(t, backupIDBase, "")

	// add more data after backup completed and do incremental backup
	numObjects2 := 250
	for i := 0; i < numObjects2; i++ {
		var b book
		require.NoError(t, gofakeit.Struct(&b))
		require.NoError(t, helper.CreateObject(t, b.toObject(s.config.ClassName)))
	}
	s.CreateBackup(t, backupIDIncremental1, backupIDBase)
	checkCount(t, endpoints, s.config.ClassName, numObjects+numObjects2)

	// add more data after backup completed and do a second incremental backup
	for i := 0; i < numObjects2; i++ {
		var b book
		require.NoError(t, gofakeit.Struct(&b))
		require.NoError(t, helper.CreateObject(t, b.toObject(s.config.ClassName)))
	}

	s.CreateBackup(t, backupIDIncremental2, backupIDIncremental1)
	checkCount(t, endpoints, s.config.ClassName, numObjects+2*numObjects2)

	helper.DeleteClass(t, s.config.ClassName)
	s.RestoreBackup(t, backupIDBase)
	checkCount(t, endpoints, s.config.ClassName, numObjects)

	helper.DeleteClass(t, s.config.ClassName)
	s.RestoreBackup(t, backupIDIncremental1)
	checkCount(t, endpoints, s.config.ClassName, numObjects+numObjects2)

	helper.DeleteClass(t, s.config.ClassName)
	s.RestoreBackup(t, backupIDIncremental2)
	checkCount(t, endpoints, s.config.ClassName, numObjects+2*numObjects2)

	// check that sizes are as expected. We return the size of the pre-compression/deduplication data in the backup status
	res1, err := helper.CreateBackupStatus(t, s.config.BackendType, backupIDBase, s.config.BucketName, "")
	require.NoError(t, err)
	res2, err := helper.CreateBackupStatus(t, s.config.BackendType, backupIDIncremental1, s.config.BucketName, "")
	require.NoError(t, err)
	res3, err := helper.CreateBackupStatus(t, s.config.BackendType, backupIDIncremental2, s.config.BucketName, "")
	require.NoError(t, err)

	if s.config.MinioEndpoint != "" {
		// verify that incremental backups are smaller than the full backup. Only on S3 backends where we can check the
		// actual stored size.
		backupSize1, err := getTotalSize(t, s.config.MinioEndpoint, s.config.BucketName, backupIDBase)
		require.NoError(t, err)

		backupSize2, err := getTotalSize(t, s.config.MinioEndpoint, s.config.BucketName, backupIDIncremental1)
		require.NoError(t, err)

		backupSize3, err := getTotalSize(t, s.config.MinioEndpoint, s.config.BucketName, backupIDIncremental2)
		require.NoError(t, err)

		t.Logf("actual backup sizes from minio: base: %d, incremental1: %d, incremental2: %d", backupSize1, backupSize2, backupSize3)
		require.Less(t, backupSize2, backupSize1)
		require.Less(t, backupSize3, backupSize1)
	}

	// total sizes should increase with each incremental backup as we add more objects
	t.Logf("Total precompression sizes: base backup size: %v, incremental1 size: %v, incremental size2: %v", res1.Payload.Size, res2.Payload.Size, res3.Payload.Size)
	require.Greater(t, res2.Payload.Size, res1.Payload.Size)
	require.Greater(t, res3.Payload.Size, res2.Payload.Size)
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

	// Enable compression if configured (must be after objects exist)
	switch s.config.WithCompression {
	case CompressionPQ:
		t.Run("enable PQ compression", func(t *testing.T) {
			s.EnablePQ(t)
		})
	case CompressionRQ:
		t.Run("enable RQ compression", func(t *testing.T) {
			s.EnableRQ(t)
		})
	case CompressionNone:
	default:
		t.Fatalf("unsupported compression type: %s", s.config.WithCompression)
	}

	t.Run("verify objects exist", func(t *testing.T) {
		s.VerifyObjectsExist(t)
	})

	t.Run("create backup", func(t *testing.T) {
		s.CreateBackup(t, backupID, "")
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

	// For compressed backups, verify vectors are restored correctly
	if s.config.WithCompression != CompressionNone {
		t.Run("verify compressed vectors restored", func(t *testing.T) {
			s.VerifyCompressedVectorsRestored(t, 3) // Sample 3 random objects
		})
	}

	t.Run("cleanup", func(t *testing.T) {
		s.DeleteTestClass(t)
	})
}

// RunCancellationTest tests that backups can be cancelled.
func (s *BackupTestSuite) RunCancellationTest(t *testing.T) {
	backupID := fmt.Sprintf("%s-cancel-%d", s.config.BackupID, time.Now().UnixNano())

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

	t.Run("create and cancel backup", func(t *testing.T) {
		cfg := helper.DefaultBackupConfig()

		// Start backup
		resp, err := helper.CreateBackup(t, cfg, s.config.ClassName, s.config.BackendType, backupID)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Cancel immediately
		t.Run("cancel backup", func(t *testing.T) {
			require.Nil(t, helper.CancelBackup(t, s.config.BackendType, backupID))
		})

		// Wait for cancellation (up to 20 seconds)
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

	wait:
		for {
			select {
			case <-ticker.C:
				break wait
			default:
				statusResp, err := helper.CreateBackupStatus(t, s.config.BackendType, backupID, "", "")
				if err == nil && statusResp != nil && statusResp.Payload != nil && statusResp.Payload.Status != nil {
					status := *statusResp.Payload.Status
					if status == string(backup.Cancelled) || status == string(backup.Success) {
						break wait
					}
				}
				time.Sleep(500 * time.Millisecond)
			}
		}

		// Verify final status is cancelled (or succeeded if backup was too fast)
		statusResp, err := helper.CreateBackupStatus(t, s.config.BackendType, backupID, "", "")
		require.NoError(t, err)
		require.NotNil(t, statusResp)
		require.NotNil(t, statusResp.Payload)
		require.NotNil(t, statusResp.Payload.Status)

		status := *statusResp.Payload.Status
		// Accept either Cancelled or Success (backup might complete before cancel)
		assert.True(t, status == string(backup.Cancelled) || status == string(backup.Success),
			"backup status should be Cancelled or Success, got: %s", status)
	})

	t.Run("cleanup", func(t *testing.T) {
		s.DeleteTestClass(t)
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

	// Run appropriate test based on configuration
	if s.config.TestCancellation {
		t.Run("backup_cancellation", func(t *testing.T) {
			s.RunCancellationTest(t)
		})
	} else if s.config.TestIncremental {
		t.Run("incremental backup restore", func(t *testing.T) {
			s.RunIncrementalTestAndRestore(t)
		})
	} else {
		t.Run("basic_backup_restore", func(t *testing.T) {
			s.RunBasicBackupRestoreTest(t)
		})
	}
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

	// WithCompression specifies the compression algorithm to enable (CompressionPQ or CompressionRQ)
	WithCompression CompressionType

	// TestCancellation tests backup cancellation instead of full backup/restore
	TestCancellation bool

	// TestIncremental tests incremental backup and restore
	TestIncremental bool
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

// SingleTenantWithPQTestCase returns a test case for single-tenant backup with PQ compression.
func SingleTenantWithPQTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "single_tenant_pq",
		MultiTenant:      false,
		ObjectsPerTenant: 10,
		WithCompression:  CompressionPQ,
	}
}

// MultiTenantWithPQTestCase returns a test case for multi-tenant backup with PQ compression.
func MultiTenantWithPQTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "multi_tenant_pq",
		MultiTenant:      true,
		NumTenants:       50,
		ObjectsPerTenant: 10,
		WithCompression:  CompressionPQ,
	}
}

// SingleTenantWithRQTestCase returns a test case for single-tenant backup with RQ compression.
func SingleTenantWithRQTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "single_tenant_rq",
		MultiTenant:      false,
		ObjectsPerTenant: 10,
		WithCompression:  CompressionRQ,
	}
}

// MultiTenantWithRQTestCase returns a test case for multi-tenant backup with RQ compression.
func MultiTenantWithRQTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "multi_tenant_rq",
		MultiTenant:      true,
		NumTenants:       50,
		ObjectsPerTenant: 10,
		WithCompression:  CompressionRQ,
	}
}

// CancellationTestCase returns a test case for testing backup cancellation.
func CancellationTestCase() BackupTestCase {
	return BackupTestCase{
		Name:             "cancellation",
		MultiTenant:      false,
		ObjectsPerTenant: 100, // More objects to give time for cancellation
		TestCancellation: true,
	}
}

// CancellationTestCase returns a test case for testing backup cancellation.
func IncrementalTestCase() BackupTestCase {
	return BackupTestCase{
		Name:            "incremental",
		MultiTenant:     false,
		TestIncremental: true,
	}
}

// SharedComposeConfig holds configuration for the shared Docker compose environment.
type SharedComposeConfig struct {
	// Compose is the shared Docker compose instance
	Compose *docker.DockerCompose
	// BackendType is the backup backend type ("s3", "gcs", "azure", "filesystem")
	BackendType string
	// MinioEndpoint is the MinIO endpoint (for S3 tests)
	MinioEndpoint string
	// Region is the S3 region (for S3 tests)
	Region string
	// GCSEndpoint is the GCS emulator endpoint (for GCS tests)
	GCSEndpoint string
	// GCSProjectID is the GCS project ID (for GCS tests)
	GCSProjectID string
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

	// Default to s3 if not specified
	backendType := sharedConfig.BackendType
	if backendType == "" {
		backendType = "s3"
	}

	// Filesystem backend requires single-node cluster
	clusterSize := 3 // Default for shared compose
	if backendType == "filesystem" {
		clusterSize = 1
	}

	return &BackupTestSuiteConfig{
		BackendType:      backendType,
		BucketName:       "backups", // Use the default bucket created by shared cluster
		Region:           sharedConfig.Region,
		ClassName:        fmt.Sprintf("Class_%s_%d", testCase.Name, timestamp),
		BackupID:         fmt.Sprintf("backup-%s-%d", testCase.Name, timestamp),
		MultiTenant:      testCase.MultiTenant,
		NumTenants:       numTenants,
		ObjectsPerTenant: objectsPerTenant,
		ClusterSize:      clusterSize,
		WithVectorizer:   true,
		TestTimeout:      5 * time.Minute,
		BackupTimeout:    2 * time.Minute,
		RestoreTimeout:   2 * time.Minute,
		ExternalCompose:  sharedConfig.Compose,
		MinioEndpoint:    sharedConfig.MinioEndpoint,
		WithCompression:  testCase.WithCompression,
		TestCancellation: testCase.TestCancellation,
		TestIncremental:  testCase.TestIncremental,
	}
}

// RunS3BackupTests runs S3 backup tests using a shared compose and test case configuration.
func RunS3BackupTests(t *testing.T, compose *docker.DockerCompose, minioEndpoint, region string, testCase BackupTestCase) {
	sharedConfig := SharedComposeConfig{
		Compose:       compose,
		BackendType:   "s3",
		MinioEndpoint: minioEndpoint,
		Region:        region,
	}

	config := NewSuiteConfigFromTestCase(sharedConfig, testCase)
	suite := NewBackupTestSuite(config)
	suite.RunTestsWithSharedCompose(t, compose)
}

// RunGCSBackupTests runs GCS backup tests using a shared compose and test case configuration.
func RunGCSBackupTests(t *testing.T, compose *docker.DockerCompose, testCase BackupTestCase) {
	sharedConfig := SharedComposeConfig{
		Compose:     compose,
		BackendType: "gcs",
	}

	config := NewSuiteConfigFromTestCase(sharedConfig, testCase)
	suite := NewBackupTestSuite(config)
	suite.RunTestsWithSharedCompose(t, compose)
}

// RunAzureBackupTests runs Azure backup tests using a shared compose and test case configuration.
func RunAzureBackupTests(t *testing.T, compose *docker.DockerCompose, testCase BackupTestCase) {
	sharedConfig := SharedComposeConfig{
		Compose:     compose,
		BackendType: "azure",
	}

	config := NewSuiteConfigFromTestCase(sharedConfig, testCase)
	suite := NewBackupTestSuite(config)
	suite.RunTestsWithSharedCompose(t, compose)
}

// RunFilesystemBackupTests runs filesystem backup tests using a shared compose and test case configuration.
// Note: Filesystem backup only works on single-node clusters. The shared compose must be a single-node cluster.
func RunFilesystemBackupTests(t *testing.T, compose *docker.DockerCompose, testCase BackupTestCase) {
	sharedConfig := SharedComposeConfig{
		Compose:     compose,
		BackendType: "filesystem",
	}

	config := NewSuiteConfigFromTestCase(sharedConfig, testCase)
	suite := NewBackupTestSuite(config)
	suite.RunTestsWithSharedCompose(t, compose)
}

func checkCount(t *testing.T, nodeEndpoints []string, classname string, numObjects int) {
	t.Helper()
	for i := range nodeEndpoints {
		helper.SetupClient(nodeEndpoints[i])
		resp, err := queryGQL(t, fmt.Sprintf("{ Aggregate { %s { meta { count } } } }", classname))
		require.NoError(t, err)
		if resp.Payload.Errors != nil {
			for _, err := range resp.Payload.Errors {
				if err != nil {
					t.Logf("GraphQL errors on node %d: %+v", i+1, resp.Payload.Errors)
				}
			}
		}
		require.Nil(t, resp.Payload.Errors, "GraphQL errors: %+v", resp.Payload.Errors)
		require.NotNil(t, resp.Payload.Data)

		countJson := resp.Payload.Data["Aggregate"].(map[string]interface{})[classname].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"].(json.Number)
		count, err := countJson.Int64()
		require.NoError(t, err)
		require.Equal(t, int64(numObjects), count, "expected all objects to be present on node %d", i+1)
	}
}

func queryGQL(t *testing.T, query string) (*gql.GraphqlPostOK, error) {
	params := gql.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: "", Query: query, Variables: nil})
	return helper.Client(t).Graphql.GraphqlPost(params, nil)
}

// getFolderChunks gets all the chunks for a given backupID from the specified S3 bucket
func getTotalSize(t *testing.T, minioURL, bucketName, backupId string) (int64, error) {
	t.Helper()

	client, err := minio.New(minioURL, &minio.Options{
		Creds: minioCredentials.NewStaticV4("aws_access_key", // MinIO default access key
			"aws_secret_key", // MinIO default secret key
			"",
		),
		Secure: false,
	})
	require.NoError(t, err)

	ctx := context.Background()

	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	totalSize := int64(0)
	for object := range objectCh {
		require.NoError(t, object.Err)
		if !strings.Contains(object.Key, backupId) {
			continue
		}
		totalSize += object.Size
	}

	return totalSize, nil
}
