//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	ucschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/schema/migrate"
	"golang.org/x/sync/errgroup"
)

type filterableToSearchableMigrator struct {
	logger       logrus.FieldLogger
	files        *filterableToSearchableMigrationFiles
	schemaGetter schema.SchemaGetter
	indexes      map[string]*Index
}

func newFilterableToSearchableMigrator(migrator *Migrator) *filterableToSearchableMigrator {
	return &filterableToSearchableMigrator{
		logger:       migrator.logger,
		files:        newFilterableToSearchableMigrationFiles(migrator.db.config.RootPath),
		schemaGetter: migrator.db.schemaGetter,
		indexes:      migrator.db.indices,
	}
}

func (m *filterableToSearchableMigrator) do(ctx context.Context, updateSchema migrate.UpdateSchema) error {
	// only properties with both Filterable and Searchable indexing enabled
	// filterable bucket has map strategy (when it should have roaring set stategy)
	// searchable bucket does not exist (in fact weaviate will create empty one with map strategy)

	// if flag exists, no class/property needs fixing
	if m.files.existsMigrationSkipFlag() {
		m.log().Debug("migration skip flag set, skipping migration")
		return nil
	}

	m.log().Debug("loading migration state")
	migrationState, err := m.files.loadMigrationState()
	if err != nil {
		m.log().WithError(err).Error("loading migrated state")
		return errors.Wrap(err, "loading migrated state")
	}

	migrationStateUpdated := false
	updateLock := new(sync.Mutex)
	sch := m.schemaGetter.GetSchemaSkipAuth().Objects

	errgrp := &errgroup.Group{}
	errgrp.SetLimit(20)
	for _, index := range m.indexes {
		index := index

		errgrp.Go(func() error {
			migratedProps, err := m.migrateClass(ctx, index, sch)
			if err != nil {
				m.logIndex(index).WithError(err).Error("failed migrating class")
				return errors.Wrap(err, "failed migrating class")
			}
			if len(migratedProps) == 0 {
				return nil
			}

			updateLock.Lock()
			defer updateLock.Unlock()

			if err := updateSchema(ctx, m.updateSchemaCallback(index, migratedProps)); err != nil {
				m.logIndex(index).WithError(err).Error("failed updating schema")
				return errors.Wrap(err, "failed updating schema")
			}

			migrationState.Class2Props[index.Config.ClassName.String()] = migratedProps
			migrationStateUpdated = true
			return nil
		})
	}

	err = errgrp.Wait()
	if err != nil {
		m.log().WithError(err).Error("failed migrating classes")
	}

	// save state regardless of previous error
	if migrationStateUpdated {
		m.log().Debug("saving migration state")
		if err := m.files.saveMigrationState(migrationState); err != nil {
			m.log().WithError(err).Error("failed saving migration state")
			return errors.Wrap(err, "failed saving migration state")
		}
	}

	if err != nil {
		return errors.Wrap(err, "failed migrating classes")
	}

	if err := m.files.createMigrationSkipFlag(); err != nil {
		m.log().WithError(err).Error("failed creating migration skip flag")
		return errors.Wrap(err, "failed creating migration skip flag")
	}

	return nil
}

func (m *filterableToSearchableMigrator) migrateClass(ctx context.Context, index *Index,
	sch *models.Schema,
) (map[string]struct{}, error) {
	m.logIndex(index).Debug("started migration of index")

	className := index.Config.ClassName.String()
	class, err := ucschema.GetClassByName(sch, className)
	if err != nil {
		return nil, err
	}

	shard2PropsToFix := map[string]map[string]struct{}{}
	uniquePropsToFix := map[string]struct{}{}
	for _, prop := range class.Properties {
		if !(inverted.IsFilterable(prop) && inverted.IsSearchable(prop)) {
			continue
		}

		for _, shard := range index.Shards {
			if toFix, err := m.isPropToFix(prop, shard); toFix {
				if _, ok := shard2PropsToFix[shard.name]; !ok {
					shard2PropsToFix[shard.name] = map[string]struct{}{}
				}
				shard2PropsToFix[shard.name][prop.Name] = struct{}{}
				uniquePropsToFix[prop.Name] = struct{}{}
			} else if err != nil {
				m.logShard(shard).WithError(err).Error("failed discovering props to fix")
				return nil, errors.Wrap(err, "failed discovering props to fix")
			}
		}
	}

	m.logIndex(index).
		WithField("number_of_props", len(uniquePropsToFix)).
		WithField("props", m.uniquePropsToSlice(uniquePropsToFix)).
		Debug("found properties to fix")

	if len(uniquePropsToFix) == 0 {
		return nil, nil
	}

	errgrp := &errgroup.Group{}
	errgrp.SetLimit(10)
	for shardName, props := range shard2PropsToFix {
		shard := index.Shards[shardName]
		props := props

		errgrp.Go(func() error {
			if err := m.migrateShard(ctx, shard, props); err != nil {
				m.logShard(shard).WithError(err).Error("failed migrating shard")
				return errors.Wrap(err, "failed migrating shard")
			}
			return nil
		})
	}
	if err := errgrp.Wait(); err != nil {
		return nil, err
	}

	m.logIndex(index).Debug("finished migration of index")
	return uniquePropsToFix, nil
}

func (m *filterableToSearchableMigrator) migrateShard(ctx context.Context, shard *Shard,
	props map[string]struct{},
) error {
	m.logShard(shard).Debug("started migration of shard")

	m.pauseStoreActivity(ctx, shard)
	defer m.resumeStoreActivity(ctx, shard)

	for propName := range props {
		srcBucketName := helpers.BucketFromPropNameLSM(propName)
		dstBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

		m.logShard(shard).
			WithField("bucketSrc", srcBucketName).
			WithField("bucketDst", dstBucketName).
			WithField("prop", propName).
			Debug("replacing buckets")

		if err := shard.store.ReplaceBuckets(ctx, dstBucketName, srcBucketName); err != nil {
			return err
		}
	}

	m.logShard(shard).Debug("finished migration of shard")
	return nil
}

func (m *filterableToSearchableMigrator) isPropToFix(prop *models.Property, shard *Shard) (bool, error) {
	bucketFilterable := shard.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name))
	if bucketFilterable != nil &&
		bucketFilterable.Strategy() == lsmkv.StrategyMapCollection &&
		bucketFilterable.DesiredStrategy() == lsmkv.StrategyRoaringSet {

		bucketSearchable := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM(prop.Name))
		if bucketSearchable != nil &&
			bucketSearchable.Strategy() == lsmkv.StrategyMapCollection {

			if m.isEmptyMapBucket(bucketSearchable) {
				return true, nil
			}
			return false, fmt.Errorf("searchable bucket is not empty")
		} else {
			return false, fmt.Errorf("searchable bucket should have map strategy")
		}
	}
	return false, nil
}

func (m *filterableToSearchableMigrator) updateSchemaCallback(index *Index,
	migratedProps map[string]struct{},
) migrate.UpdateSchemaCallback {
	return func(sch *models.Schema) error {
		m.logIndex(index).Debug("updating schema")

		class, err := ucschema.GetClassByName(sch, index.Config.ClassName.String())
		if err != nil {
			return err
		}

		propsToUpdate := []*models.Property{}
		for propName := range migratedProps {
			property, err := ucschema.GetPropertyByName(class, propName)
			if err != nil {
				return err
			}
			propsToUpdate = append(propsToUpdate, property)
		}

		// update all props at once, when there was no errors
		vFalse := false
		for _, property := range propsToUpdate {
			property.IndexFilterable = &vFalse
		}

		return nil
	}
}

func (m *filterableToSearchableMigrator) isEmptyMapBucket(bucket *lsmkv.Bucket) bool {
	cur := bucket.MapCursorKeyOnly()
	defer cur.Close()

	key, _ := cur.First()
	return key == nil
}

func (m *filterableToSearchableMigrator) pauseStoreActivity(
	ctx context.Context, shard *Shard,
) error {
	m.logShard(shard).Debug("pausing store activity")

	if err := shard.store.PauseCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed pausing compaction for shard '%s'", shard.ID())
	}
	if err := shard.store.FlushMemtables(ctx); err != nil {
		return errors.Wrapf(err, "failed flushing memtables for shard '%s'", shard.ID())
	}
	shard.store.UpdateBucketsStatus(storagestate.StatusReadOnly)

	m.logShard(shard).Debug("paused store activity")
	return nil
}

func (m *filterableToSearchableMigrator) resumeStoreActivity(
	ctx context.Context, shard *Shard,
) error {
	m.logShard(shard).Debug("resuming store activity")

	if err := shard.store.ResumeCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed resuming compaction for shard '%s'", shard.ID())
	}
	shard.store.UpdateBucketsStatus(storagestate.StatusReady)

	m.logShard(shard).Debug("resumed store activity")
	return nil
}

func (m *filterableToSearchableMigrator) log() *logrus.Entry {
	return m.logger.WithField("action", "inverted filter2search migration")
}

func (m *filterableToSearchableMigrator) logIndex(index *Index) *logrus.Entry {
	return m.log().WithField("index", index.ID())
}

func (m *filterableToSearchableMigrator) logShard(shard *Shard) *logrus.Entry {
	return m.logIndex(shard.index).WithField("shard", shard.ID())
}

func (m *filterableToSearchableMigrator) uniquePropsToSlice(uniqueProps map[string]struct{}) []string {
	props := make([]string, 0, len(uniqueProps))
	for prop := range uniqueProps {
		props = append(props, prop)
	}
	return props
}

type filterableToSearchableMigrationState struct {
	Class2Props map[string]map[string]struct{}
}

type filterableToSearchableMigrationFiles struct {
	flagFileName  string
	stateFileName string
}

func newFilterableToSearchableMigrationFiles(rootPath string) *filterableToSearchableMigrationFiles {
	return &filterableToSearchableMigrationFiles{
		flagFileName:  path.Join(rootPath, "migration1.19.filter2search.skip.flag"),
		stateFileName: path.Join(rootPath, "migration1.19.filter2search.state"),
	}
}

func (mf *filterableToSearchableMigrationFiles) loadMigrationState() (*filterableToSearchableMigrationState, error) {
	f, err := os.OpenFile(mf.stateFileName, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(f); err != nil {
		return nil, err
	}
	bytes := buf.Bytes()

	state := filterableToSearchableMigrationState{
		Class2Props: map[string]map[string]struct{}{},
	}

	if len(bytes) > 0 {
		if err := json.Unmarshal(bytes, &state); err != nil {
			return nil, err
		}
	}
	return &state, nil
}

func (mf *filterableToSearchableMigrationFiles) saveMigrationState(state *filterableToSearchableMigrationState) error {
	bytes, err := json.Marshal(state)
	if err != nil {
		return err
	}

	fileNameTemp := mf.stateFileName + ".temp"
	f, err := os.Create(fileNameTemp)
	if err != nil {
		return err
	}

	_, err = f.Write(bytes)
	f.Close()
	if err != nil {
		return err
	}

	err = os.Rename(fileNameTemp, mf.stateFileName)
	if err != nil {
		return err
	}
	return nil
}

func (mf *filterableToSearchableMigrationFiles) existsMigrationSkipFlag() bool {
	_, err := os.Stat(mf.flagFileName)
	return err == nil
}

func (mf *filterableToSearchableMigrationFiles) createMigrationSkipFlag() error {
	f, err := os.Create(mf.flagFileName)
	if err != nil {
		return err
	}
	f.Close()
	return nil
}
