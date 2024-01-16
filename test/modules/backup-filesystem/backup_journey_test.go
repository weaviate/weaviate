//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
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
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("backup-filesystem", func(t *testing.T) {
			journey.BackupJourneyTests_SingleNode(t, compose.GetWeaviate().URI(),
				"filesystem", fsBackupJourneyClassName, fsBackupJourneyBackupIDSingleNode, nil)
		})
	})

	t.Run("multiple nodes", func(t *testing.T) {
		compose, err := docker.New().
			WithBackendFilesystem().
			WithText2VecContextionary().
			WithWeaviateCluster().
			Start(ctx)
		require.Nil(t, err)

		defer func() {
			if err := compose.Terminate(ctx); err != nil {
				t.Fatalf("failed to terminate test containers: %s", err.Error())
			}
		}()

		t.Run("backup-filesystem", func(t *testing.T) {
			journey.BackupJourneyTests_Cluster(t, "filesystem",
				fsBackupJourneyClassName, fsBackupJourneyBackupIDCluster, nil,
				compose.GetWeaviate().URI(), compose.GetWeaviateNode2().URI())
		})
	})
}
