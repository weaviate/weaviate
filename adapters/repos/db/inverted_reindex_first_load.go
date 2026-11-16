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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// MigrationDecidedRunSource reads the task manager as of the current RAFT apply.
type MigrationDecidedRunSource func(collection, shard, node string) []distributedtask.DecidedRun

// SetMigrationDecidedRunSource must run before the first shard loads.
func (db *DB) SetMigrationDecidedRunSource(source MigrationDecidedRunSource) {
	db.migrationDecidedRuns = source
}

var errFirstLoadRecord = errors.New("write the migration records of a shard's first load")

// writeFirstLoadRecords migrates a new shard past runs that already swapped, as
// no swap will reach it. Keyed on the objects bucket so a retry still counts.
func (s *Shard) writeFirstLoadRecords(class *models.Class) error {
	unit := s.migrationUnit()
	if unit == "" || s.index.db == nil || s.index.db.migrationDecidedRuns == nil {
		return nil
	}
	runs := s.index.db.migrationDecidedRuns(class.Class, s.name, s.index.getSchema.NodeName())
	if len(runs) == 0 {
		return nil
	}
	holds, err := dirHoldsFile(filepath.Join(s.pathLSM(), helpers.ObjectsBucketLSM))
	if err != nil {
		return fmt.Errorf("%w: %w", errFirstLoadRecord, err)
	}
	if holds {
		return nil
	}
	// The record store does not create the LSM directory.
	if err := os.Mkdir(s.pathLSM(), os.ModePerm); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("%w: %w", errFirstLoadRecord, err)
	}
	store := NewMigrationRecordStoreForUnit(s.pathLSM(), unit, s.index.logger)
	if err := store.Load(); err != nil {
		return fmt.Errorf("%w: %w", errFirstLoadRecord, err)
	}
	for _, run := range runs {
		records, err := firstLoadRecords(s.index.logger, run, unit, class)
		if err != nil {
			return fmt.Errorf("%w: %w", errFirstLoadRecord, err)
		}
		for _, rec := range records {
			if _, there := store.Get(rec.Subject().Key); there {
				continue
			}
			if err := store.Put(rec); err != nil {
				return fmt.Errorf("%w: %w", errFirstLoadRecord, err)
			}
		}
	}
	return nil
}

func firstLoadRecords(logger logrus.FieldLogger, run distributedtask.DecidedRun, unit string,
	class *models.Class,
) ([]MigrationRecord, error) {
	header, ok := run.Header.(ReindexTaskHeader)
	if !ok {
		return nil, fmt.Errorf("task %s/%d carries no reindex header", run.ID, run.Version)
	}
	payload := ReindexTaskPayload{
		MigrationType: header.MigrationType, Collection: header.Class, Properties: header.Properties,
		TargetTokenization: header.TargetTokenization, BucketStrategy: header.BucketStrategy,
	}
	tasks, err := buildMigrationTasks(logger, nil, run.Version, &payload, func(_, propName string) bool {
		prop, err := schema.GetPropertyByName(class, propName)
		return err == nil && inverted.HasFilterableIndex(prop)
	})
	if err != nil {
		return nil, err
	}
	records := make([]MigrationRecord, 0, len(tasks))
	for _, task := range tasks {
		subject := MigrationSubject{
			Key:           MigrationRecordKey{TaskVersion: run.Version, StrategyCode: task.strategy.StrategyCode(), UnitID: unit},
			TaskID:        run.ID,
			MigrationType: header.MigrationType, TargetTokenization: header.TargetTokenization,
			Collection: header.Class, BucketStrategy: header.BucketStrategy,
			Props: make(map[string]MigrationPropertyDirs, len(header.Properties)),
		}
		for _, prop := range header.Properties {
			subject.Props[prop] = MigrationPropertyDirs{Canonical: task.strategy.SourceBucketName(prop)}
		}
		records = append(records, NewMigrationRecordPromoted(subject, subject.Properties(), nil))
	}
	return records, nil
}

func dirHoldsFile(dir string) (bool, error) {
	holds := false
	err := filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			holds = true
			return fs.SkipAll
		}
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return holds, err
}

// A first-load bucket has no segment recording its blockmax format, so the
// promoted record decides it until the schema does.
func (s *Shard) promotedToBlockmax(propName string) bool {
	if s.migrationRecords == nil {
		return false
	}
	for _, rec := range s.migrationRecords.Records() {
		_, covers := rec.Subject().Props[propName]
		if covers && rec.State() == MigrationStatePromoted &&
			rec.Subject().Key.StrategyCode == StrategyCodeSearchableMapToBlockmax {
			return true
		}
	}
	return false
}
