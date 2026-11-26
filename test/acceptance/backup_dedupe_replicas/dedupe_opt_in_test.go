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

package backup_dedupe_replicas_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	ubak "github.com/weaviate/weaviate/usecases/backup"
	"github.com/weaviate/weaviate/usecases/license"
)

func errorMessages(payload *models.ErrorResponse) string {
	if payload == nil {
		return ""
	}
	messages := make([]string, 0, len(payload.Error))
	for _, item := range payload.Error {
		messages = append(messages, item.Message)
	}
	return strings.Join(messages, "; ")
}

func TestBackupDedupeRequiresOptIn(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateLicense().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	const className = "OptInArticles"
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{{Name: "contents", DataType: []string{"text"}}},
	})
	defer helper.DeleteClass(t, className)

	_, err = helper.CreateBackup(t, dedupeBackupConfig(), className, "filesystem", "opt-in-backup")
	require.Error(t, err)
	var uerr *backups.BackupsCreateUnprocessableEntity
	require.True(t, errors.As(err, &uerr), "want 422, got %T: %v", err, err)
	assert.Contains(t, errorMessages(uerr.Payload), "BACKUP_DEDUPE_ENABLED")
}

func TestBackupDedupeRequiresLicense(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendS3(bucketName, regionName).
		WithWeaviate().
		WithWeaviateEnv("BACKUP_DEDUPE_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	host := compose.GetWeaviate().URI()
	helper.SetupClient(host)
	defer helper.ResetClient()

	const (
		className = "LicenseArticles"
		plainID   = "license-plain-backup"
	)
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{{Name: "contents", DataType: []string{"text"}}},
	})
	defer helper.DeleteClass(t, className)
	seedObjects(t, host, className, 50)

	t.Run("dedupe create is refused with the license text and the docs URL", func(t *testing.T) {
		_, err := helper.CreateBackup(t, dedupeBackupConfig(), className, backendS3, "license-dedupe-backup")
		require.Error(t, err)
		var ferr *backups.BackupsCreateForbidden
		require.True(t, errors.As(err, &ferr), "want 403, got %T: %v", err, err)
		assert.Equal(t, license.Required(ubak.DedupeFeature).Error()+", see "+license.EnterpriseDocsURL, errorMessages(ferr.Payload))
	})

	t.Run("a plain backup still succeeds as a legacy artifact", func(t *testing.T) {
		_, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), className, backendS3, plainID)
		require.NoError(t, err)
		helper.ExpectBackupEventuallyCreated(t, plainID, backendS3, nil, helper.WithDeadline(4*time.Minute))

		global := readGlobalMeta(t, minioClient(t, compose.GetMinIO().URI()), plainID)
		assert.Equal(t, ubak.Version, global.Version)
		assert.False(t, global.DedupeReplicas)
	})
}
