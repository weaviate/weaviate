//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func waitForBackup(t *testing.T, backupID, backend string) {
	for {
		resp, err := helper.CreateBackupStatus(t, backend, backupID, "", "")
		require.Nil(t, err)
		require.NotNil(t, resp.Payload)
		if *resp.Payload.Status == "SUCCESS" {
			break
		}
		if *resp.Payload.Status == "FAILED" {
			t.Fatalf("backup failed: %s", resp.Payload.Error)
		}
		time.Sleep(time.Second / 10)
	}
}

func Test_AliasesAPI_Backup(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithBackendFilesystem().
		WithWeaviate().
		WithText2VecModel2Vec().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Run("create schema", func(t *testing.T) {
		t.Run("Books", func(t *testing.T) {
			booksClass := books.ClassModel2VecVectorizer()
			helper.CreateClass(t, booksClass)
			for _, book := range books.Objects() {
				helper.CreateObject(t, book)
				helper.AssertGetObjectEventually(t, book.Class, book.ID)
			}
		})
	})

	var aliases []string
	t.Run("create aliases", func(t *testing.T) {
		tests := []struct {
			name  string
			alias *models.Alias
		}{
			{
				name:  books.DefaultClassName,
				alias: &models.Alias{Alias: "BookAlias", Class: books.DefaultClassName},
			},
			{
				name:  books.DefaultClassName,
				alias: &models.Alias{Alias: "BookAliasToBeDeleted", Class: books.DefaultClassName},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				helper.CreateAlias(t, tt.alias)
				resp := helper.GetAliases(t, &tt.alias.Class)
				require.NotNil(t, resp)
				require.NotEmpty(t, resp.Aliases)
				aliasCreated := false
				for _, alias := range resp.Aliases {
					if tt.alias.Alias == alias.Alias && tt.alias.Class == alias.Class {
						aliasCreated = true
					}
				}
				assert.True(t, aliasCreated)
				aliases = append(aliases, tt.alias.Alias)
			})
		}
	})

	defer func() {
		resp := helper.GetAliases(t, nil)
		require.NotNil(t, resp)
		for _, alias := range resp.Aliases {
			helper.DeleteAlias(t, alias.Alias)
		}
		helper.DeleteClass(t, books.DefaultClassName)
	}()

	t.Run("delete alias", func(t *testing.T) {
		checkAliasesCount := func(t *testing.T, count int) {
			resp := helper.GetAliases(t, nil)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, count, len(resp.Aliases))
		}
		checkAliasesCount(t, 2)
		helper.DeleteAlias(t, "BookAliasToBeDeleted")
		checkAliasesCount(t, 1)
	})

	backend := "filesystem"
	backupID := "backup-1_aliases"

	t.Run("backup with local filesystem backend", func(t *testing.T) {
		backupResp, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), books.DefaultClassName, backend, backupID)
		assert.Nil(t, err)
		assert.NotNil(t, backupResp)
		waitForBackup(t, backupID, backend)
	})

	t.Run("delete collection", func(t *testing.T) {
		helper.DeleteClass(t, books.DefaultClassName)
	})

	t.Run("delete alias", func(t *testing.T) {
		helper.DeleteAlias(t, "BookAlias")
	})

	t.Run("check alias count after deletion", func(t *testing.T) {
		resp := helper.GetAliases(t, nil)
		require.NotNil(t, resp)
		require.Empty(t, resp.Aliases)
	})

	t.Run("restore with local filesystem backend", func(t *testing.T) {
		restoreResp, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), books.DefaultClassName, backend, backupID, map[string]string{})
		assert.Nil(t, err)
		assert.NotNil(t, restoreResp)
	})

	t.Run("check class after restore", func(t *testing.T) {
		resp := helper.GetClass(t, books.DefaultClassName)
		require.NotNil(t, resp)
	})

	t.Run("check alias count after restore", func(t *testing.T) {
		checkAliasesCount := func(t *testing.T, count int) {
			resp := helper.GetAliases(t, nil)
			require.NotNil(t, resp)
			require.NotEmpty(t, resp.Aliases)
			require.Equal(t, count, len(resp.Aliases))
		}
		checkAliasesCount(t, 1)
	})
}
