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

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
)

type propertyDeleteIndexHelper struct{}

func newPropertyDeleteIndexHelper() *propertyDeleteIndexHelper {
	return &propertyDeleteIndexHelper{}
}

// markPropertyIndexRemoved creates a flag file to record that a property's index was removed.
func (p *propertyDeleteIndexHelper) markPropertyIndexRemoved(indexPath, propertyName string) error {
	migrationDirectory := p.migrationDirectory(indexPath)
	if _, err := os.Stat(migrationDirectory); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat migrations directory: %w", err)
		}
		if err := os.Mkdir(migrationDirectory, os.FileMode(0o755)); err != nil {
			return fmt.Errorf("failed to create index migrations directory: %w", err)
		}
	}
	flagFile := p.propertyUpdatedFlagFile(migrationDirectory, propertyName)
	if _, err := os.Stat(flagFile); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat flag file for property %s: %w", propertyName, err)
		}
		file, err := os.Create(flagFile)
		if err != nil {
			return fmt.Errorf("failed to create property updated flag file for property %s: %w", propertyName, err)
		}
		defer file.Close()
	}
	return nil
}

// ensureBucketsAreRemovedForNonExistentPropertyIndexes removes property buckets
// for nonexistent property indexes which may be left on disk in two cases:
// - tenant was inactive during drop property index operation hence their property buckets may still exist on disk
// - an error occurred during update property operation and most probably property buckets haven't been removed
func (p *propertyDeleteIndexHelper) ensureBucketsAreRemovedForNonExistentPropertyIndexes(
	indexPath, shardName string, class *models.Class,
) error {
	migrationDirectory := p.migrationDirectory(indexPath)
	if _, err := os.Stat(migrationDirectory); err == nil {
		files, err := os.ReadDir(migrationDirectory)
		if err != nil {
			return fmt.Errorf("failed to read files in directory %s: %w", migrationDirectory, err)
		}
		var propertyNames []string
		for _, entry := range files {
			if !entry.IsDir() {
				filename := entry.Name()
				if strings.HasSuffix(filename, p.propertyUpdatedFlagFileSuffix()) {
					propertyNames = append(propertyNames, p.getPropertyName(filename))
				}
			}
		}
		if len(propertyNames) > 0 {
			for _, prop := range class.Properties {
				if slices.Contains(propertyNames, prop.Name) {
					if p.isPropertyIndexRemoved(prop.IndexFilterable) && p.propertyIndexBucketExistsOnDisk(indexPath, shardName, helpers.BucketFromPropNameLSM(prop.Name)) {
						if err := p.removePropertyIndexBucketFromDisk(indexPath, shardName, helpers.BucketFromPropNameLSM(prop.Name)); err != nil {
							return fmt.Errorf("failed to remove unused bucket for filterable index: class %s property %s: %w", class.Class, prop.Name, err)
						}
					}
					if p.isPropertyIndexRemoved(prop.IndexSearchable) && p.propertyIndexBucketExistsOnDisk(indexPath, shardName, helpers.BucketSearchableFromPropNameLSM(prop.Name)) {
						if err := p.removePropertyIndexBucketFromDisk(indexPath, shardName, helpers.BucketSearchableFromPropNameLSM(prop.Name)); err != nil {
							return fmt.Errorf("failed to remove unused bucket for searchable index: class %s property %s: %w", class.Class, prop.Name, err)
						}
					}
					if p.isPropertyIndexRemoved(prop.IndexRangeFilters) && p.propertyIndexBucketExistsOnDisk(indexPath, shardName, helpers.BucketRangeableFromPropNameLSM(prop.Name)) {
						if err := p.removePropertyIndexBucketFromDisk(indexPath, shardName, helpers.BucketRangeableFromPropNameLSM(prop.Name)); err != nil {
							return fmt.Errorf("failed to remove unused bucket for rangeFilters index: class %s property %s: %w", class.Class, prop.Name, err)
						}
					}
				}
			}
		}
	}
	return nil
}

func (p *propertyDeleteIndexHelper) migrationDirectory(indexPath string) string {
	return filepath.Join(indexPath, ".migrations")
}

func (p *propertyDeleteIndexHelper) propertyUpdatedFlagFile(migrationDirectory, propertyName string) string {
	return filepath.Join(migrationDirectory, fmt.Sprintf("%s%s", propertyName, p.propertyUpdatedFlagFileSuffix()))
}

func (p *propertyDeleteIndexHelper) propertyUpdatedFlagFileSuffix() string {
	return ".index.removed"
}

func (p *propertyDeleteIndexHelper) getPropertyName(filename string) string {
	return strings.TrimSuffix(filename, p.propertyUpdatedFlagFileSuffix())
}

func (p *propertyDeleteIndexHelper) getPropertyIndexDir(indexPath, shardName, propertyIndexName string) string {
	return filepath.Join(indexPath, shardName, "lsm", propertyIndexName)
}

func (p *propertyDeleteIndexHelper) isPropertyIndexRemoved(propertyIndexSetting *bool) bool {
	return propertyIndexSetting != nil && !*propertyIndexSetting
}

func (p *propertyDeleteIndexHelper) propertyIndexBucketExistsOnDisk(indexPath, shardName, propertyIndexName string) bool {
	propertyIndexBucketPath := p.getPropertyIndexDir(indexPath, shardName, propertyIndexName)
	if _, err := os.Stat(propertyIndexBucketPath); err == nil {
		return true
	}
	return false
}

func (p *propertyDeleteIndexHelper) removePropertyIndexBucketFromDisk(indexPath, shardName, propertyIndexName string) error {
	propertyIndexBucketPath := p.getPropertyIndexDir(indexPath, shardName, propertyIndexName)
	if err := os.RemoveAll(propertyIndexBucketPath); err != nil {
		return fmt.Errorf("failed to remove data for nonexistent property index: %s: %w", propertyIndexBucketPath, err)
	}
	return nil
}
