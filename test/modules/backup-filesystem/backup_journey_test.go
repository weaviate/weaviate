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
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	modstgfilesystem "github.com/weaviate/weaviate/modules/backup-filesystem"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/journey"
)

const (
	fsBackupJourneyClassName          = "FileSystemBackup"
	fsBackupJourneyBackupIDSingleNode = "fs-backup-single-node"
	fsBackupJourneyBackupIDCluster    = "fs-backup-cluster"
)

func Test_BackupJourney(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t.Run("single node", func(t *testing.T) {
		compose, err := docker.New().
			WithBackendFilesystem().
			WithText2VecContextionary().
			WithWeaviate().
			WithWeaviateEnv("BACKUP_CHUNK_TARGET_SIZE", "10").
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("backup-filesystem", func(t *testing.T) {
			defer printLogsOnError(t, compose)
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"filesystem", fsBackupJourneyClassName, fsBackupJourneyBackupIDSingleNode, nil, false, "", "")
		})
	})

	t.Run("single node", func(t *testing.T) {
		compose, err := docker.New().
			WithBackendFilesystem().
			WithText2VecContextionary().
			WithWeaviateEnv("ENABLE_CLEANUP_UNFINISHED_BACKUPS", "true").
			WithWeaviateEnv("BACKUP_CHUNK_TARGET_SIZE", "10").
			WithWeaviate().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("backup-filesystem", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"filesystem", fsBackupJourneyClassName, fsBackupJourneyBackupIDSingleNode, nil, true, "testbucketoverride", "testBucketPathOverride")
		})

		t.Run("cancel after restart", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
			journey.CancelFromRestartJourney(t, compose, compose.GetWeaviate().Name(), modstgfilesystem.Name)
		})
	})
}

func printLogsOnError(t *testing.T, compose *docker.DockerCompose) {
	if !t.Failed() {
		return
	}

	// When a test fails, dump logs of all compose containers.
	for _, container := range compose.Containers() {
		logs, err := container.Container().Logs(context.Background())
		if err != nil {
			t.Logf("failed to get logs for container %s: %v", container.Name(), err)
			continue
		}
		func() {
			defer logs.Close()
			t.Logf("=== start for container %s ===\n=== start logs ===", container.Name())

			scanner := bufio.NewScanner(logs)

			for scanner.Scan() {
				line := scanner.Text()
				t.Log(line)
			}
			t.Logf("=== logs for container %s ===\n=== end logs ===", container.Name())
		}()
	}
}
