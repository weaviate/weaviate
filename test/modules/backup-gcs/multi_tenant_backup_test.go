package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
	moduleshelper "github.com/weaviate/weaviate/test/helper/modules"
)

const numTenants = 50

func Test_MultiTenantBackupJourney_SingleNode_Default(t *testing.T) {
	runMultiTenantBackupSingleNode(t, false, "backups", "", "")
}

func Test_MultiTenantBackupJourney_SingleNode_Override(t *testing.T) {
	runMultiTenantBackupSingleNode(t, true, "backups", "gcsmbjtestbucketoverride", "testBucketPathOverride")
}

func Test_MultiTenantBackupJourney_MultiNode_Default(t *testing.T) {
	runMultiTenantBackupMultiNode(t, false, "backups", "", "")
}

func Test_MultiTenantBackupJourney_MultiNode_Override(t *testing.T) {
	runMultiTenantBackupMultiNode(t, true, "backups", "gcsmbjtestbucketoverride", "testBucketPathOverride")
}

func runMultiTenantBackupSingleNode(t *testing.T, override bool, bucket, overrideBucket, overridePath string) {
	ctx := context.Background()
	gcsBackupJourneyBucketName := bucket
	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	compose, err := docker.New().
		WithBackendGCS(gcsBackupJourneyBucketName).
		WithText2VecContextionary().
		WithWeaviateEnv("EXPERIMENTAL_BACKWARDS_COMPATIBLE_NAMED_VECTORS", "true").
		WithWeaviateEnv(envGCSCredentials, "").
		WithWeaviateEnv(envGCSProjectID, gcsBackupJourneyProjectID).
		WithWeaviateEnv(envGCSBucket, gcsBackupJourneyBucketName).
		WithWeaviate().
		Start(ctx)
	require.Nil(t, err)
	defer compose.Terminate(ctx)

	t.Setenv(envGCSEndpoint, compose.GetGCS().URI())
	t.Setenv(envGCSStorageEmulatorHost, compose.GetGCS().URI())
	moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
	moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, "gcsmbjtestbucketoverride")
	defer moduleshelper.DeleteGCSBucket(ctx, t, gcsBackupJourneyBucketName)
	defer moduleshelper.DeleteGCSBucket(ctx, t, "gcsmbjtestbucketoverride")

	helper.SetupClient(compose.GetWeaviate().URI())
	journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
		"gcs", gcsBackupJourneyClassName,
		gcsBackupJourneyBackupIDSingleNode, tenantNames, override, overrideBucket, overridePath)
}

func runMultiTenantBackupMultiNode(t *testing.T, override bool, bucket, overrideBucket, overridePath string) {
	ctx := context.Background()
	gcsBackupJourneyBucketName := bucket
	tenantNames := make([]string, numTenants)
	for i := range tenantNames {
		tenantNames[i] = fmt.Sprintf("Tenant%d", i)
	}

	compose, err := docker.New().
		WithBackendGCS(gcsBackupJourneyBucketName).
		WithText2VecContextionary().
		WithWeaviateEnv("EXPERIMENTAL_BACKWARDS_COMPATIBLE_NAMED_VECTORS", "true").
		WithWeaviateEnv(envGCSCredentials, "").
		WithWeaviateEnv(envGCSProjectID, gcsBackupJourneyProjectID).
		WithWeaviateEnv(envGCSBucket, gcsBackupJourneyBucketName).
		WithWeaviateCluster(3).
		Start(ctx)
	require.Nil(t, err)
	defer compose.Terminate(ctx)

	t.Setenv(envGCSEndpoint, compose.GetGCS().URI())
	t.Setenv(envGCSStorageEmulatorHost, compose.GetGCS().URI())
	moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, gcsBackupJourneyBucketName)
	moduleshelper.CreateGCSBucket(ctx, t, gcsBackupJourneyProjectID, "gcsmbjtestbucketoverride")
	defer moduleshelper.DeleteGCSBucket(ctx, t, gcsBackupJourneyBucketName)
	defer moduleshelper.DeleteGCSBucket(ctx, t, "gcsmbjtestbucketoverride")

	helper.SetupClient(compose.GetWeaviate().URI())
	journey.BackupJourneyTests_Cluster(t, "gcs", gcsBackupJourneyClassName,
		gcsBackupJourneyBackupIDCluster+overrideBucket, tenantNames, override, overrideBucket, overridePath,
		compose.GetWeaviate().URI(), compose.GetWeaviateNode(2).URI())
}
