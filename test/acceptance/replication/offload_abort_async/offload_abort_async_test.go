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

package offload_abort_async

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	envS3AccessKey = "AWS_ACCESS_KEY_ID"
	envS3SecretKey = "AWS_SECRET_KEY"
	s3AccessKey    = "aws_access_key"
	s3SecretKey    = "aws_secret_key"
)

func i64(v int64) *int64 { return &v }

// TestFreezeAbortDoesNotSilentlyDivergeReplicas: after a failed tenant freeze, a write on only some replicas must still converge everywhere.
func TestFreezeAbortDoesNotSilentlyDivergeReplicas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	t.Setenv(envS3AccessKey, s3AccessKey)
	t.Setenv(envS3SecretKey, s3SecretKey)

	// OFFLOAD_TIMEOUT bounds the failing upload once MinIO is down. Disable bucket
	// auto-create (MinIO pre-creates the bucket) so a node can restart while MinIO is
	// down — auto-create is the only thing that makes the offload module touch MinIO at init.
	compose, err := docker.New().
		WithOffloadS3("offloading", "us-west-1").
		WithWeaviateEnv("OFFLOAD_TIMEOUT", "5").
		WithoutWeaviateEnvs("OFFLOAD_S3_BUCKET_AUTO_CREATE").
		WithText2VecContextionary().
		With3NodeCluster().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	className := "OffloadAbortAsyncClass"
	tenant := "Tenant1"
	testClass := models.Class{
		Class:             className,
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:              true,
			AutoTenantActivation: true,
		},
		// fast async cadence so repair is observable
		Properties: []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	}
	testClass.ReplicationConfig.AsyncConfig = &models.ReplicationAsyncConfig{
		Frequency:                 i64(1000),
		FrequencyWhilePropagating: i64(1000),
		PropagationDelay:          i64(1000),
	}
	helper.CreateClass(t, &testClass)
	defer helper.DeleteClass(t, className)
	helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})

	baselineIDs := []strfmt.UUID{
		"00000000-0000-0000-0000-0000000000a1",
		"00000000-0000-0000-0000-0000000000a2",
		"00000000-0000-0000-0000-0000000000a3",
	}
	for _, id := range baselineIDs {
		require.Nil(t, helper.CreateObject(t, &models.Object{
			ID: id, Class: className, Tenant: tenant,
			Properties: map[string]interface{}{"name": "baseline"},
		}))
	}

	nodeNames := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

	// baseline present on every replica before we start
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, node := range nodeNames {
			for _, id := range baselineIDs {
				_, err := common.GetTenantObjectFromNode(t, compose.GetWeaviate().URI(), className, id, node, tenant)
				assert.NoErrorf(ct, err, "baseline object %s must be present on %s", id, node)
			}
		}
	}, 120*time.Second, 3*time.Second, "baseline must converge on all replicas")

	// bring the offload backend down so the freeze upload fails
	require.Nil(t, compose.StopMinIO(ctx))

	// freeze with MinIO down: the upload fails (surfaced synchronously) while the abort reverts the tenant to HOT
	if err := helper.UpdateTenantsReturnError(t, className, []*models.Tenant{
		{Name: tenant, ActivityStatus: models.TenantActivityStatusFROZEN},
	}); err != nil {
		t.Logf("FROZEN update surfaced the expected offload error: %v", err)
	}
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := helper.GetTenants(t, className)
		require.Nil(t, err)
		for _, tn := range resp.Payload {
			if tn.Name == tenant {
				assert.Equal(ct, models.TenantActivityStatusHOT, tn.ActivityStatus)
			}
		}
	}, 90*time.Second, 2*time.Second, "tenant must revert to HOT after the aborted freeze")

	// stop a replica (weaviate-2; StopNode is 0-based), write W while down, restart it (snapshot-reload path)
	require.Nil(t, compose.StopNode(ctx, 2, nil))
	const w = strfmt.UUID("77777777-7777-7777-7777-777777777777")
	require.Nil(t, common.CreateObjectCL(t, compose.GetWeaviate().URI(), &models.Object{
		ID: w, Class: className, Tenant: tenant,
		Properties: map[string]interface{}{"name": "w"},
	}, types.ConsistencyLevelOne))
	require.Nil(t, compose.StartNode(ctx, 2))

	// async restored → W and baseline converge everywhere
	wantIDs := append(append([]strfmt.UUID{}, baselineIDs...), w)
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, node := range nodeNames {
			for _, id := range wantIDs {
				_, err := common.GetTenantObjectFromNode(t, compose.GetWeaviate().URI(), className, id, node, tenant)
				assert.NoErrorf(ct, err, "object %s must be present on %s", id, node)
			}
		}
	}, 180*time.Second, 3*time.Second, "with async replication restored, W must converge to every replica")
}
