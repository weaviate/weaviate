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

package test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_FreezeUnderStorageOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
	t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

	compose, err := docker.New().
		WithOffloadS3("offloading", "us-west-1").
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() { _ = compose.Terminate(context.Background()) }()
	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		className = "FreezeOutage"
		tenant    = "Tenant1"
	)
	helper.CreateClass(t, &models.Class{
		Class:              className,
		Vectorizer:         "none",
		ReplicationConfig:  &models.ReplicationConfig{Factor: 2},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	})
	helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}, {Name: "Warmup"}})
	ids := []strfmt.UUID{
		"00000000-0000-0000-0000-0000000000c1",
		"00000000-0000-0000-0000-0000000000c2",
	}
	for _, id := range ids {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			ID: id, Class: className, Tenant: tenant, Properties: map[string]interface{}{"name": "kept"},
		}))
	}

	client := helper.Client(t)
	awaitStatus := func(t *testing.T, name, status string, timeout time.Duration) *models.Tenant {
		t.Helper()
		var got *models.Tenant
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Schema.TenantsGetOne(clschema.NewTenantsGetOneParams().
				WithContext(ctx).WithClassName(className).WithTenantName(name), nil)
			require.NoError(c, err)
			got = resp.Payload
			assert.Equal(c, status, got.ActivityStatus)
		}, timeout, 500*time.Millisecond)
		return got
	}
	setStatus := func(name, status string) {
		helper.UpdateTenants(t, className, []*models.Tenant{{Name: name, ActivityStatus: status}})
	}

	// a freeze caches its bucket check, so one runs while storage is up
	setStatus("Warmup", models.TenantActivityStatusFROZEN)
	awaitStatus(t, "Warmup", models.TenantActivityStatusFROZEN, 30*time.Second)
	require.NoError(t, compose.PauseMinIO(ctx))
	setStatus(tenant, models.TenantActivityStatusFROZEN)

	var participants []*models.TenantTransitionParticipant
	t.Run("the tenant reports its freeze", func(t *testing.T) {
		got := awaitStatus(t, tenant, models.TenantActivityStatusFREEZING, 10*time.Second)
		require.NotNil(t, got.Transition)
		assert.Equal(t, "FREEZE", got.Transition.Kind)
		assert.Equal(t, models.TenantActivityStatusFROZEN, got.Transition.TargetStatus)
		require.Len(t, got.Transition.Participants, 2)
		for _, participant := range got.Transition.Participants {
			assert.Equal(t, "STARTED", participant.Progress, participant.Node)
		}
		participants = got.Transition.Participants
	})

	t.Run("a freeze survives a participant restart", func(t *testing.T) {
		restarted := -1
		for _, participant := range participants {
			// weaviate-0 serves the client, so it stays up
			if n, ok := strings.CutPrefix(participant.Node, "weaviate-"); ok && n != "0" {
				restarted, err = strconv.Atoi(n)
				require.NoError(t, err)
			}
		}
		require.NotEqual(t, -1, restarted, "a participant other than weaviate-0")

		require.NoError(t, compose.StopNode(ctx, restarted, nil))
		require.NoError(t, compose.UnpauseMinIO(ctx))
		require.NoError(t, compose.StartNode(ctx, restarted))
		awaitStatus(t, tenant, models.TenantActivityStatusFROZEN, 3*time.Minute)

		setStatus(tenant, models.TenantActivityStatusHOT)
		awaitStatus(t, tenant, models.TenantActivityStatusHOT, 2*time.Minute)
		for _, id := range ids {
			_, err := helper.TenantObject(t, className, id, tenant)
			require.NoError(t, err, id)
		}
	})
}

// A kill with no grace runs no shutdown, so the restart must finish from the staged state.
func Test_UnfreezeSurvivesAKilledParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	t.Setenv(envS3AccessKey, s3BackupJourneyAccessKey)
	t.Setenv(envS3SecretKey, s3BackupJourneySecretKey)

	compose, err := docker.New().
		WithOffloadS3("offloading", "us-west-1").
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() { _ = compose.Terminate(context.Background()) }()
	helper.SetupClient(compose.GetWeaviate().URI())

	const (
		className = "UnfreezeKill"
		tenant    = "Tenant1"
	)
	helper.CreateClass(t, &models.Class{
		Class:              className,
		Vectorizer:         "none",
		ReplicationConfig:  &models.ReplicationConfig{Factor: 2},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         []*models.Property{{Name: "name", DataType: schema.DataTypeText.PropString()}},
	})
	helper.CreateTenants(t, className, []*models.Tenant{{Name: tenant}})
	ids := []strfmt.UUID{
		"00000000-0000-0000-0000-0000000000d1",
		"00000000-0000-0000-0000-0000000000d2",
		"00000000-0000-0000-0000-0000000000d3",
	}
	for _, id := range ids {
		require.NoError(t, helper.CreateObject(t, &models.Object{
			ID: id, Class: className, Tenant: tenant, Properties: map[string]interface{}{"name": "kept"},
		}))
	}

	client := helper.Client(t)
	awaitStatus := func(t *testing.T, status string, timeout time.Duration) *models.Tenant {
		t.Helper()
		var got *models.Tenant
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := client.Schema.TenantsGetOne(clschema.NewTenantsGetOneParams().
				WithContext(ctx).WithClassName(className).WithTenantName(tenant), nil)
			require.NoError(c, err)
			got = resp.Payload
			assert.Equal(c, status, got.ActivityStatus)
		}, timeout, 500*time.Millisecond)
		return got
	}
	setStatus := func(status string) {
		helper.UpdateTenants(t, className, []*models.Tenant{{Name: tenant, ActivityStatus: status}})
	}

	setStatus(models.TenantActivityStatusFROZEN)
	awaitStatus(t, models.TenantActivityStatusFROZEN, time.Minute)

	// the downloads hang until storage is back
	require.NoError(t, compose.PauseMinIO(ctx))
	setStatus(models.TenantActivityStatusHOT)
	got := awaitStatus(t, models.TenantActivityStatusUNFREEZING, 30*time.Second)
	require.NotNil(t, got.Transition)

	killed := -1
	for _, participant := range got.Transition.Participants {
		// weaviate-0 serves the client, so it stays up
		if n, ok := strings.CutPrefix(participant.Node, "weaviate-"); ok && n != "0" {
			killed, err = strconv.Atoi(n)
			require.NoError(t, err)
		}
	}
	require.NotEqual(t, -1, killed, "a participant other than weaviate-0")

	noGrace := time.Duration(0)
	require.NoError(t, compose.StopNode(ctx, killed, &noGrace))
	require.NoError(t, compose.UnpauseMinIO(ctx))
	require.NoError(t, compose.StartNode(ctx, killed))

	awaitStatus(t, models.TenantActivityStatusHOT, 3*time.Minute)
	for _, id := range ids {
		_, err := helper.TenantObject(t, className, id, tenant)
		require.NoError(t, err, id)
	}
}
