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

func (m *filterableToSearchableMigrator) migrate(ctx context.Context) error {
	// only properties with both Filterable and Searchable indexing enabled
	// filterable bucket has map strategy (when it should have roaring set strategy)
	// searchable bucket does not exist (in fact weaviate will create empty one with map strategy)

	// if flag exists, no class/property needs fixing
	if m.files.existsMigrationSkipFlag() {
		m.log().Debug("migration skip flag set, skipping migration")
		return nil
	}

	migrationState, err := m.files.loadMigrationState()
	if err != nil {
		m.log().WithError(err).Error("loading migrated state")
		return errors.Wrap(err, "loading migrated state")
	}

	migrationStateUpdated := false
	updateLock := new(sync.Mutex)
	sch := m.schemaGetter.GetSchemaSkipAuth().Objects

	m.log().Debug("starting migration")

	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU * 2)
	for _, index := range m.indexes {
		index := index

		eg.Go(func() error {
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

			migrationState.MissingFilterableClass2Props[index.Config.ClassName.String()] = migratedProps
			migrationStateUpdated = true
			return nil
		})
	}

	err = eg.Wait()
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

	m.log().Debug("finished migration")
	return nil
}

func (m *filterableToSearchableMigrator) switchShardsToFallbackMode(ctx context.Context) error {
	m.log().Debug("starting switching fallback mode")

	migrationState, err := m.files.loadMigrationState()
	if err != nil {
		m.log().WithError(err).Error("loading migrated state")
		return errors.Wrap(err, "loading migrated state")
	}

	if len(migrationState.MissingFilterableClass2Props) == 0 {
		m.log().Debug("no missing filterable indexes, fallback mode skipped")
		return nil
	}

	for _, index := range m.indexes {
		if _, ok := migrationState.MissingFilterableClass2Props[index.Config.ClassName.String()]; !ok {
			continue
		}
		index.ForEachShard(func(name string, shard ShardLike) error {
			m.logShard(shard).Debug("setting fallback mode for shard")
			shard.setFallbackToSearchable(true)
			return nil
		})
	}

	m.log().Debug("finished switching fallback mode")
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
		if !(inverted.HasFilterableIndex(prop) && inverted.HasSearchableIndex(prop)) {
			continue
		}
		if err := index.ForEachShard(func(name string, shard ShardLike) error {
			if toFix, err := m.isPropToFix(prop, shard); toFix {
				if _, ok := shard2PropsToFix[shard.Name()]; !ok {
					shard2PropsToFix[shard.Name()] = map[string]struct{}{}
				}
				shard2PropsToFix[shard.Name()][prop.Name] = struct{}{}
				uniquePropsToFix[prop.Name] = struct{}{}
			} else if err != nil {
				m.logShard(shard).WithError(err).Error("failed discovering props to fix")
				return errors.Wrap(err, "failed discovering props to fix")
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	m.logIndex(index).
		WithField("number_of_props", len(uniquePropsToFix)).
		WithField("props", m.uniquePropsToSlice(uniquePropsToFix)).
		Debug("found properties to fix")

	if len(uniquePropsToFix) == 0 {
		return nil, nil
	}

	eg := &errgroup.Group{}
	eg.SetLimit(_NUMCPU)
	for shardName, props := range shard2PropsToFix {
		shard := index.shards.Load(shardName)
		props := props

		eg.Go(func() error {
			if err := m.migrateShard(ctx, shard, props); err != nil {
				m.logShard(shard).WithError(err).Error("failed migrating shard")
				return errors.Wrap(err, "failed migrating shard")
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	m.logIndex(index).Debug("finished migration of index")
	return uniquePropsToFix, nil
}

func (m *filterableToSearchableMigrator) migrateShard(ctx context.Context, shard ShardLike,
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

		if err := shard.Store().ReplaceBuckets(ctx, dstBucketName, srcBucketName); err != nil {
			return err
		}
	}

	m.logShard(shard).Debug("finished migration of shard")
	return nil
}

func (m *filterableToSearchableMigrator) isPropToFix(prop *models.Property, shard ShardLike) (bool, error) {
	bucketFilterable := shard.Store().Bucket(helpers.BucketFromPropNameLSM(prop.Name))
	if bucketFilterable != nil &&
		bucketFilterable.Strategy() == lsmkv.StrategyMapCollection &&
		bucketFilterable.DesiredStrategy() == lsmkv.StrategyRoaringSet {

		bucketSearchable := shard.Store().Bucket(helpers.BucketSearchableFromPropNameLSM(prop.Name))
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

func (m *filterableToSearchableMigrator) isEmptyMapBucket(bucket *lsmkv.Bucket) bool {
	cur := bucket.MapCursorKeyOnly()
	defer cur.Close()

	key, _ := cur.First()
	return key == nil
}

func (m *filterableToSearchableMigrator) pauseStoreActivity(
	ctx context.Context, shard ShardLike,
) error {
	m.logShard(shard).Debug("pausing store activity")

	if err := shard.Store().PauseCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed pausing compaction for shard '%s'", shard.ID())
	}
	if err := shard.Store().FlushMemtables(ctx); err != nil {
		return errors.Wrapf(err, "failed flushing memtables for shard '%s'", shard.ID())
	}
	shard.Store().UpdateBucketsStatus(storagestate.StatusReadOnly)

	m.logShard(shard).Debug("paused store activity")
	return nil
}

func (m *filterableToSearchableMigrator) resumeStoreActivity(
	ctx context.Context, shard ShardLike,
) error {
	m.logShard(shard).Debug("resuming store activity")

	if err := shard.Store().ResumeCompaction(ctx); err != nil {
		return errors.Wrapf(err, "failed resuming compaction for shard '%s'", shard.ID())
	}
	shard.Store().UpdateBucketsStatus(storagestate.StatusReady)

	m.logShard(shard).Debug("resumed store activity")
	return nil
}

func (m *filterableToSearchableMigrator) log() *logrus.Entry {
	return m.logger.WithField("action", "inverted filter2search migration")
}

func (m *filterableToSearchableMigrator) logIndex(index *Index) *logrus.Entry {
	return m.log().WithField("index", index.ID())
}

func (m *filterableToSearchableMigrator) logShard(shard ShardLike) *logrus.Entry {
	return m.logIndex(shard.Index()).WithField("shard", shard.ID())
}

func (m *filterableToSearchableMigrator) uniquePropsToSlice(uniqueProps map[string]struct{}) []string {
	props := make([]string, 0, len(uniqueProps))
	for prop := range uniqueProps {
		props = append(props, prop)
	}
	return props
}

type filterableToSearchableMigrationState struct {
	MissingFilterableClass2Props map[string]map[string]struct{}
	CreatedFilterableClass2Props map[string]map[string]struct{}
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
		MissingFilterableClass2Props: map[string]map[string]struct{}{},
		CreatedFilterableClass2Props: map[string]map[string]struct{}{},
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
