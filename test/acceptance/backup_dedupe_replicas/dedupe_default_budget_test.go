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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBackupDedupeDefaultBudget(t *testing.T) {
	const className = "DefaultBudgetArticles"
	cases := []struct {
		name          string
		env           string
		requestBudget int64
		wantBudget    string
	}{
		{name: "built-in default", wantBudget: "15s"},
		{name: "env sets the default", env: "7s", wantBudget: "7s"},
		{name: "request overrides the env default", env: "7s", requestBudget: 9, wantBudget: "9s"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			builder := docker.New().
				WithBackendFilesystem().
				WithWeaviate().
				WithWeaviateEnv("BACKUP_DEDUPE_ENABLED", "true").
				WithWeaviateLicense()
			if tc.env != "" {
				builder = builder.WithWeaviateEnv("BACKUP_DEDUPE_CONVERGENCE_TIMEOUT", tc.env)
			}
			compose, err := builder.Start(ctx)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, compose.Terminate(ctx))
			}()

			helper.SetupClient(compose.GetWeaviate().URI())
			defer helper.ResetClient()
			helper.CreateClass(t, &models.Class{Class: className, Vectorizer: "none"})

			cfg := helper.DefaultBackupConfig()
			cfg.DedupeReplicas = true
			cfg.DedupeConvergenceTimeoutSeconds = tc.requestBudget
			backupID := fmt.Sprintf("default-budget-%d", i)
			resp, err := helper.CreateBackup(t, cfg, className, "filesystem", backupID)
			require.NoError(t, err)
			require.NotNil(t, resp.Payload)

			needle := fmt.Sprintf(`"budget":"%s"`, tc.wantBudget)
			require.EventuallyWithT(t, func(ct *assert.CollectT) {
				logs, err := compose.GetWeaviate().Container().Logs(ctx)
				require.NoError(ct, err)
				buf, err := io.ReadAll(logs)
				if cerr := logs.Close(); cerr != nil {
					t.Logf("close logs: %v", cerr)
				}
				require.NoError(ct, err)
				require.Contains(ct, string(buf), needle)
			}, time.Minute, time.Second)
		})
	}
}
