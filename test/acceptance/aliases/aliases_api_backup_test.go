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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
)

func testAliasesAPIBackup(t *testing.T) {
	// Three options for the test:
	// 1. full: backup and restore collection after deleting both collection and the alias, will pass as of 1.32
	// 2. overwrite: backup and restore after deleting the collection but not the alias, should pass only if "overwrite option is set"
	tests := []struct {
		option string
	}{
		{option: "full"},
		{option: "overwrite"},
	}
	for _, tt := range tests {
		t.Run("backup with "+tt.option, func(t *testing.T) {
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
						alias: &models.Alias{Alias: "BackupBookAlias", Class: books.DefaultClassName},
					},
					{
						name:  books.DefaultClassName,
						alias: &models.Alias{Alias: "BackupBookAliasToBeDeleted", Class: books.DefaultClassName},
					},
					{
						name:  books.DefaultClassName,
						alias: &models.Alias{Alias: "BackupBookAliasOverwrite", Class: books.DefaultClassName},
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
				helper.DeleteClass(t, "Books2")
			}()

			t.Run("delete alias", func(t *testing.T) {
				checkAliasesCount := func(t *testing.T, count int) {
					require.Equal(t, count, countAliasesWithPrefix(t, "Backup"))
				}
				checkAliasesCount(t, 3)
				helper.DeleteAlias(t, "BackupBookAliasToBeDeleted")
				checkAliasesCount(t, 2)
			})

			backend := "filesystem"
			backupID := tt.option + "-backup-id"

			t.Run("backup with local filesystem backend", func(t *testing.T) {
				backupResp, err := helper.CreateBackup(t, helper.DefaultBackupConfig(), books.DefaultClassName, backend, backupID)
				assert.Nil(t, err)
				assert.NotNil(t, backupResp)
				helper.ExpectBackupEventuallyCreated(t, backupID, backend, nil, helper.WithPollInterval(helper.MinPollInterval), helper.WithDeadline(helper.MaxDeadline))
			})

			if tt.option == "overwrite" {
				t.Run("reassign BookAliasOverwrite from Books to Books2 class", func(t *testing.T) {
					alias := helper.GetAlias(t, "BackupBookAliasOverwrite")
					assert.Equal(t, "Books", alias.Class)
					// create Books2 class
					books2Class := books.ClassModel2VecVectorizerWithName("Books2")
					helper.CreateClass(t, books2Class)
					for _, book := range books.ObjectsWithName("Books2") {
						helper.CreateObject(t, book)
						helper.AssertGetObjectEventually(t, book.Class, book.ID)
					}
					helper.UpdateAlias(t, "BackupBookAliasOverwrite", "Books2")
				})
			}

			t.Run("delete collection", func(t *testing.T) {
				helper.DeleteClass(t, books.DefaultClassName)
			})

			if tt.option != "overwrite" {
				t.Run("delete alias", func(t *testing.T) {
					helper.DeleteAlias(t, "BackupBookAlias")
					helper.DeleteAlias(t, "BackupBookAliasOverwrite")
				})

				t.Run("check alias count after deletion", func(t *testing.T) {
					require.Equal(t, 0, countAliasesWithPrefix(t, "Backup"))
				})
			}

			if tt.option == "overwrite" {
				t.Run("check BookAliasOverwrite alias that it points to Books2 before restore", func(t *testing.T) {
					alias := helper.GetAlias(t, "BackupBookAliasOverwrite")
					assert.Equal(t, "Books2", alias.Class)
				})
			}

			t.Run("restore with local filesystem backend", func(t *testing.T) {
				var overwriteAlias bool
				if tt.option == "overwrite" {
					overwriteAlias = true
				}

				restoreResp, err := helper.RestoreBackup(t, helper.DefaultRestoreConfig(), books.DefaultClassName, backend, backupID, map[string]string{}, overwriteAlias)
				assert.Nil(t, err)
				assert.NotNil(t, restoreResp)
				helper.ExpectBackupEventuallyRestored(t, backupID, backend, nil, helper.WithPollInterval(helper.MinPollInterval), helper.WithDeadline(helper.MaxDeadline))
			})

			t.Run("check class after restore", func(t *testing.T) {
				resp := helper.GetClass(t, books.DefaultClassName)
				require.NotNil(t, resp)
			})

			t.Run("check alias count after restore", func(t *testing.T) {
				require.Equal(t, 2, countAliasesWithPrefix(t, "Backup"))
			})

			t.Run("check BookAliasOverwrite alias that it points to Books", func(t *testing.T) {
				alias := helper.GetAlias(t, "BackupBookAliasOverwrite")
				assert.Equal(t, books.DefaultClassName, alias.Class)
			})
		})
	}
}
