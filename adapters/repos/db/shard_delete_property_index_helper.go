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

	"github.com/sirupsen/logrus"

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
//
// A bucket a migration completed on this shard is left alone even where the
// schema says the index is disabled: between a local swap and the
// cluster-wide flag flip the two disagree on purpose, and the flag is the
// stale half. Consulted only for a bucket this sweep would otherwise delete —
// see [completedMigrationShield].
func (p *propertyDeleteIndexHelper) ensureBucketsAreRemovedForNonExistentPropertyIndexes(
	indexPath, shardName string, class *models.Class, logger logrus.FieldLogger,
) error {
	shield := newCompletedMigrationShield(filepath.Join(indexPath, shardName, "lsm"), logger)
	for _, prop := range class.Properties {
		for _, index := range []struct {
			indexType string
			label     string
			setting   *bool
			bucket    string
		}{
			{"filterable", "filterable", prop.IndexFilterable, helpers.BucketFromPropNameLSM(prop.Name)},
			{"searchable", "searchable", prop.IndexSearchable, helpers.BucketSearchableFromPropNameLSM(prop.Name)},
			{"rangeable", "rangeFilters", prop.IndexRangeFilters, helpers.BucketRangeableFromPropNameLSM(prop.Name)},
		} {
			if !p.isPropertyIndexRemoved(index.setting) {
				continue
			}
			if !p.propertyIndexBucketExistsOnDisk(indexPath, shardName, index.bucket) {
				continue
			}
			if shield.protects(prop.Name, index.indexType) {
				continue
			}
			if err := p.removePropertyIndexBucketFromDisk(indexPath, shardName, index.bucket); err != nil {
				return fmt.Errorf("failed to remove unused bucket for %s index: class %s property %s: %w",
					index.label, class.Class, prop.Name, err)
			}
		}
	}
	return nil
}

// completedMigrationShield answers whether a property index bucket the
// startup sweep is about to delete belongs to a migration that completed on
// this shard — either promoted already (`finalized.mig`) or promoted at this
// same start (`tidied.mig` / `merged.mig`, whose promotion runs later in
// shard init).
//
// The sweep runs on every shard load over every property × three index types,
// and this answer costs a directory walk, so it is asked only where the sweep
// would otherwise delete — and the first such question is what reads the
// shard's migrations directory at all. One shield per shard load shares that
// listing and the tracker payloads it attributes.
type completedMigrationShield struct {
	lsmPath    string
	logger     logrus.FieldLogger
	dirs       *dirNamesCache
	props      *taskPropsCache
	asked      bool
	unreadable bool
}

func newCompletedMigrationShield(lsmPath string, logger logrus.FieldLogger) *completedMigrationShield {
	return &completedMigrationShield{
		lsmPath: lsmPath, logger: logger,
		dirs: &dirNamesCache{}, props: &taskPropsCache{},
	}
}

func (s *completedMigrationShield) protects(propName, indexType string) bool {
	if !s.asked {
		s.asked = true
		migrationsDir := filepath.Join(s.lsmPath, ".migrations")
		if _, err := s.dirs.list(migrationsDir); err != nil && !os.IsNotExist(err) {
			// A listing that cannot be read cannot rule out a completed
			// migration, and deleting on that answer is the loss this shield
			// exists for.
			s.logger.WithField("path", migrationsDir).
				Warnf("shard init: unable to read migrations dir; leaving disabled property index buckets in place: %v", err)
			s.unreadable = true
		}
	}
	if s.unreadable {
		return true
	}
	scope := migrationDirsOf(s.lsmPath, s.dirs, propName, indexType).
		cachingProps(s.props).preserving(indexType)
	return len(completedMigrationGens(scope)) > 0
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
