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
	"slices"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// MigrationBarrierRunSource reads the task manager as of the current RAFT apply.
type MigrationBarrierRunSource func(namespace, collection, shard string) []distributedtask.BarrierRun

// SetMigrationBarrierRunSource must run before the first shard loads.
func (db *DB) SetMigrationBarrierRunSource(source MigrationBarrierRunSource) {
	db.migrationBarrierRuns = source
}

var errFirstLoadRecord = errors.New("write promoted migration records for an empty shard")

// writeFirstLoadRecords writes the promoted records of each run that already
// swapped this shard's group, since no swap reaches a new replica. It writes
// while the objects bucket holds no file, so a retry after a failed write still writes.
func (s *Shard) writeFirstLoadRecords(class *models.Class) error {
	unit := s.migrationUnit()
	if unit == "" || s.index.db == nil || s.index.db.migrationBarrierRuns == nil {
		return nil
	}
	runs, err := firstLoadRuns(s.index.db.migrationBarrierRuns(ReindexNamespace, class.Class, s.name))
	if err != nil || len(runs) == 0 {
		return err
	}
	holds, err := dirHoldsFile(filepath.Join(s.pathLSM(), helpers.ObjectsBucketLSM))
	if err != nil || holds {
		return err
	}
	if err := os.MkdirAll(s.pathLSM(), os.ModePerm); err != nil {
		return err
	}
	store := NewMigrationRecordStoreForUnit(s.pathLSM(), unit, s.index.logger)
	if err := store.Load(); err != nil {
		return err
	}
	for _, run := range runs {
		records, err := firstLoadRecords(s.index.logger, run, unit, class)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if _, there := store.Get(rec.Subject().Key); there {
				continue
			}
			if err := store.Put(rec); err != nil {
				return err
			}
		}
	}
	return nil
}

type firstLoadRun struct {
	desc   distributedtask.TaskDescriptor
	header ReindexTaskHeader
}

// firstLoadRuns keeps unfinished runs that decided the group, on the properties
// no newer finished run flipped (as ReindexMigratedTenants does).
func firstLoadRuns(runs []distributedtask.BarrierRun) ([]firstLoadRun, error) {
	headers := make([]ReindexTaskHeader, len(runs))
	for i, run := range runs {
		header, ok := run.Header.(ReindexTaskHeader)
		if !ok {
			return nil, fmt.Errorf("task %s/%d carries no reindex header", run.ID, run.Version)
		}
		headers[i] = header
	}
	var kept []firstLoadRun
	for i, run := range runs {
		if !run.GroupDecided || run.Status == distributedtask.TaskStatusFinished {
			continue
		}
		types, _ := ReindexIndexTypes(headers[i].MigrationType)
		header := headers[i]
		header.Properties = slices.DeleteFunc(slices.Clone(header.Properties), func(prop string) bool {
			for j := i + 1; j < len(runs); j++ {
				if slices.Contains(headers[j].Properties, prop) &&
					reindexFlipCovers(runs[j].Status, headers[j].MigrationType, types) {
					return true
				}
			}
			return false
		})
		if len(header.Properties) > 0 {
			kept = append(kept, firstLoadRun{desc: run.TaskDescriptor, header: header})
		}
	}
	return kept, nil
}

func firstLoadRecords(logger logrus.FieldLogger, run firstLoadRun, unit string, class *models.Class,
) ([]MigrationRecord, error) {
	payload := run.header.payload()
	tasks, err := buildMigrationTasks(logger, nil, run.desc.Version, &payload, func(_, propName string) bool {
		prop, err := schema.GetPropertyByName(class, propName)
		return err == nil && inverted.HasFilterableIndex(prop)
	})
	if err != nil {
		return nil, err
	}
	records := make([]MigrationRecord, 0, len(tasks))
	for _, task := range tasks {
		task.setMigrationIdentity(run.desc, unit, &payload)
		subject := task.migrationSubject(nil, payload.Properties, time.Time{})
		for prop, dirs := range subject.Props {
			subject.Props[prop] = MigrationPropertyDirs{Canonical: dirs.Canonical}
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

// promotedToBlockmax reports whether the newest promoted record on propName's
// searchable bucket builds it as blockmax. A bucket built on an empty shard has
// no segment recording its format, so the record keeps it until the schema does.
func (s *Shard) promotedToBlockmax(propName string) bool {
	if s.migrationRecords == nil {
		return false
	}
	all := s.migrationRecords.Records()
	for _, rec := range all {
		subject := rec.Subject()
		if rec.State() != MigrationStatePromoted ||
			subject.Props[propName].Canonical != helpers.BucketSearchableFromPropNameLSM(propName) ||
			migrationPropertySuperseded(all, subject, propName) {
			continue
		}
		return migrationTargetStrategy(s.index.logger, subject) == lsmkv.StrategyInverted
	}
	return false
}

func migrationTargetStrategy(logger logrus.FieldLogger, subject MigrationSubject) string {
	payload := subject.reindexPayload()
	tasks, err := buildMigrationTasks(logger, nil, subject.Key.TaskVersion, &payload,
		func(string, string) bool { return true })
	if err != nil {
		return ""
	}
	for _, task := range tasks {
		if task.strategy.StrategyCode() == subject.Key.StrategyCode {
			return task.strategy.TargetStrategy()
		}
	}
	return ""
}
