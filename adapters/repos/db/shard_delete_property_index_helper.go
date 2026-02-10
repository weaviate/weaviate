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

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
)

type propertyDeleteIndexHelper struct{}

func newPropertyDeleteIndexHelper() *propertyDeleteIndexHelper {
	return &propertyDeleteIndexHelper{}
}

// ensureBucketsAreRemovedForNonExistentPropertyIndexes removes property buckets
// for nonexistent property indexes which may be left on disk in two cases:
// - tenant was inactive during drop property index operation hence their property buckets may still exist on disk
// - an error occurred during update property operation and most probably property buckets haven't been removed
func (p *propertyDeleteIndexHelper) ensureBucketsAreRemovedForNonExistentPropertyIndexes(
	indexPath, shardName string, class *models.Class,
) error {
	for _, prop := range class.Properties {
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
	return nil
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
