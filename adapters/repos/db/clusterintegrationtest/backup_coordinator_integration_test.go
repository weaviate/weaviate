//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package clusterintegrationtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/backup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var backend *fakeBackupBackend

func TestDistributedBackups(t *testing.T) {
	var (
		dirName  = setupDirectory(t)
		numObjs  = 100
		numNodes = 3
		backupID = "new-backup"
		nodes    []*node
	)

	t.Run("setup", func(t *testing.T) {
		overallShardState := multiShardState(numNodes)
		shardStateSerialized, err := json.Marshal(overallShardState)
		require.Nil(t, err)

		backend = &fakeBackupBackend{
			backupsPath: dirName,
			backupID:    backupID,
			startedAt:   time.Now(),
		}

		for i := 0; i < numNodes; i++ {
			node := &node{
				name: fmt.Sprintf("node-%d", i),
			}

			node.init(dirName, shardStateSerialized, &nodes)
			nodes = append(nodes, node)
		}
	})

	t.Run("apply schema", func(t *testing.T) {
		for i := range nodes {
			err := nodes[i].migrator.AddClass(context.Background(), class(),
				nodes[i].schemaManager.shardState)
			require.Nil(t, err)
			err = nodes[i].migrator.AddClass(context.Background(), secondClassWithRef(),
				nodes[i].schemaManager.shardState)
			require.Nil(t, err)
			nodes[i].schemaManager.schema.Objects.Classes = append(nodes[i].schemaManager.schema.Objects.Classes,
				class(), secondClassWithRef())
		}
	})

	data := exampleData(numObjs)
	refData := exampleDataWithRefs(numObjs, 5, data)

	t.Run("import data", func(t *testing.T) {
		t.Run("import first class into random node", func(t *testing.T) {
			for _, obj := range data {
				node := nodes[rand.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector)
				require.Nil(t, err)
			}
		})

		t.Run("import second class into random node", func(t *testing.T) {
			for _, obj := range refData {
				node := nodes[rand.Intn(len(nodes))]

				err := node.repo.PutObject(context.Background(), obj, obj.Vector)
				require.Nil(t, err)
			}
		})
	})

	t.Run("let each node be the coordinator", func(t *testing.T) {
		for _, node := range nodes {
			t.Run(fmt.Sprintf("%s: coordinate backup", node.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				req := &backup.BackupRequest{ID: backupID, Backend: "fake-backend",
					Include: []string{distributedClass}}

				resp, err := node.scheduler.Backup(ctx, &models.Principal{}, req)
				assert.Nil(t, err, "expected nil err, got: %s", err)
				assert.Empty(t, resp.Error, "expected empty, got: %s", resp.Error)
				assert.NotEmpty(t, resp.Path)
				assert.Contains(t, resp.Classes, distributedClass)
			})

			t.Run(fmt.Sprintf("%s: get backup status", node.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				start := time.Now()

				for {
					if time.Now().After(start.Add(30 * time.Second)) {
						t.Fatal("backup deadline exceeded")
					}
					resp, err := node.scheduler.BackupStatus(ctx, &models.Principal{}, "fake-backend", backupID)
					assert.Nil(t, err, "expected nil err, got: %s", err)
					if resp != nil && string(resp.Status) == "SUCCESS" {
						break
					}
					if resp != nil && string(resp.Status) == "FAILED" {
						t.Fatalf("backup failed: %q", resp.Err)
					}
				}
			})

			t.Run(fmt.Sprintf("%s: restore to cluster", node.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				req := &backup.BackupRequest{ID: backupID, Backend: "fake-backend",
					Include: []string{distributedClass}}

				resp, err := node.scheduler.Restore(ctx, &models.Principal{}, req)
				assert.Nil(t, err, "expected nil err, got: %s", err)
				assert.Empty(t, resp.Error, "expected empty, got: %s", resp.Error)
				assert.NotEmpty(t, resp.Path)
				assert.Contains(t, resp.Classes, distributedClass)
			})

			t.Run(fmt.Sprintf("%s: get restore status", node.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				start := time.Now()

				for {
					if time.Now().After(start.Add(30 * time.Second)) {
						t.Fatal("restore deadline exceeded")
					}
					resp, err := node.scheduler.RestorationStatus(ctx, &models.Principal{}, "fake-backend", backupID)
					assert.Nil(t, err, "expected nil err, got: %s", err)
					if resp != nil && string(resp.Status) == "SUCCESS" {
						break
					}
					if resp != nil && string(resp.Status) == "FAILED" {
						t.Fatalf("restore failed: %q", resp.Err)
					}
				}
			})

			backend.reset()
		}
	})

	t.Run("shutdown", func(t *testing.T) {
		for _, node := range nodes {
			err := node.repo.Shutdown(context.Background())
			require.Nil(t, err, "expected nil, got: %v", err)
		}
	})
}
