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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/backups"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBackupDedupeKillSwitch(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithWeaviateEnv("BACKUP_DEDUPE_DISABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	const className = "KillSwitchArticles"
	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{{Name: "contents", DataType: []string{"text"}}},
	})
	defer helper.DeleteClass(t, className)

	_, err = helper.CreateBackup(t, dedupeBackupConfig(), className, "filesystem", "kill-switch-backup")
	require.Error(t, err)
	var uerr *backups.BackupsCreateUnprocessableEntity
	require.True(t, errors.As(err, &uerr), "want 422, got %T: %v", err, err)
	messages := make([]string, 0, len(uerr.Payload.Error))
	for _, item := range uerr.Payload.Error {
		messages = append(messages, item.Message)
	}
	assert.Contains(t, strings.Join(messages, "; "), "BACKUP_DEDUPE_DISABLED")
}
