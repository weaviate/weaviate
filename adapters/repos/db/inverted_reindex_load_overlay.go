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
	"cmp"
	"slices"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/logrusext"
)

func migrationEnablesIndexType(mt ReindexMigrationType) (string, bool) {
	switch mt {
	case ReindexTypeEnableSearchable:
		return "searchable", true
	case ReindexTypeEnableFilterable:
		return "filterable", true
	case ReindexTypeEnableRangeable:
		return "rangeable", true
	default:
		return "", false
	}
}

func migrationOverridesSchema(rec MigrationRecord, tasks []distributedtask.TaskStatusEntry, readable bool) bool {
	if !rec.FlipDecided() {
		return false
	}
	if !readable {
		return true
	}
	task, found := findMigrationTask(rec.Subject(), tasks)
	return found && task.Status != distributedtask.TaskStatusFinished
}

func (s *Shard) restoreMigratedOverlays(class *models.Class, unbuildableFaults *logrusext.Sampler) {
	if s.migrationRecords == nil || class == nil {
		return
	}
	tasks, readable := s.migrations().LocalTasks()
	all := s.migrationRecords.Records()
	records := slices.SortedFunc(slices.Values(all), func(a, b MigrationRecord) int {
		return cmp.Compare(a.Subject().Key.TaskVersion, b.Subject().Key.TaskVersion)
	})
	unbuildable := errorcompounder.New()
	defer s.reportUnbuildableOverlays(unbuildableFaults, unbuildable)
	for _, rec := range records {
		subject := rec.Subject()
		if !migrationOverridesSchema(rec, tasks, readable) {
			continue
		}
		if !readable {
			s.migrationRecords.overridesUnsure.Store(true)
		}
		payload := subject.reindexPayload()
		built, err := buildMigrationTasks(s.index.logger, nil, subject.Key.TaskVersion, &payload,
			func(string, string) bool { return true })
		if err != nil {
			unbuildable.AddWrapf(err, "%s", subject.Key)
			continue
		}
		built = slices.DeleteFunc(built, func(t *ShardReindexTaskGeneric) bool {
			return t.strategy.StrategyCode() != subject.Key.StrategyCode
		})
		views := propertyOverlayViews(&payload, built)
		if len(views) == 0 || views[0] == nil {
			continue
		}
		swapped, isSwapped := rec.(MigrationRecordSwapped)
		for _, prop := range subject.Properties() {
			if migrationPropertySuperseded(all, subject, prop) {
				continue
			}
			// Checks the promotion, not subject.Unmirrored: a load never promotes a stale copy over live data.
			if isSwapped && swapped.PromotionOf(prop) != migrationPromotionFinished {
				continue
			}
			var live *models.Property
			if i := slices.IndexFunc(class.Properties, func(p *models.Property) bool {
				return p != nil && p.Name == prop
			}); i >= 0 {
				live = class.Properties[i]
			}
			s.SetPropertyOverlay(prop, views[0][prop].BeyondLiveSchema(live))
		}
	}
}

func (s *Shard) reportUnbuildableOverlays(faults *logrusext.Sampler, unbuildable errorcompounder.ErrorCompounder) {
	reported := unbuildable.ToErrorLimited(maxReportedErrors)
	if reported == nil {
		return
	}
	report := func(logrus.FieldLogger) {
		s.index.logger.WithField("shard", s.name).WithField("migrations", unbuildable.Len()).
			Errorf("cannot rebuild the index settings %d migration(s) of this shard migrated to: %v",
				unbuildable.Len(), reported)
	}
	if faults == nil {
		report(nil)
		return
	}
	faults.WithSampling(report)
}

func (s *Shard) migratedIndexStands(propName, indexType string) bool {
	if s.migrationRecords == nil {
		return false
	}
	tasks, readable := s.migrations().LocalTasks()
	return migratedIndexStandsIn(s.migrationRecords.Records(), tasks, readable, propName, indexType)
}

func migratedIndexStandsIn(records []MigrationRecord, tasks []distributedtask.TaskStatusEntry, readable bool,
	propName, indexType string,
) bool {
	for _, rec := range records {
		subject := rec.Subject()
		enabled, ok := migrationEnablesIndexType(subject.MigrationType)
		if !ok || enabled != indexType || !migrationOverridesSchema(rec, tasks, readable) {
			continue
		}
		if _, covers := subject.Props[propName]; !covers {
			continue
		}
		if swapped, isSwapped := rec.(MigrationRecordSwapped); subject.Unmirrored && isSwapped &&
			swapped.PromotionOf(propName) != migrationPromotionFinished {
			continue
		}
		return true
	}
	return false
}

// Every flip-decided record stands: a newer run records its flip before it sets
// its overlay, so under this lock its record is seen or its overlay lands after.
func (s *Shard) retireReleasedOverlays(released []MigrationSubject, class *models.Class) {
	s.propertyOverlayMu.Lock()
	defer s.propertyOverlayMu.Unlock()
	records := s.migrationRecords.Records()
	for _, prop := range class.Properties {
		if prop == nil || !slices.ContainsFunc(released, func(subject MigrationSubject) bool {
			_, covers := subject.Props[prop.Name]
			return covers
		}) {
			continue
		}
		for _, indexType := range disabledIndexTypes(prop) {
			if !migratedIndexStandsIn(records, nil, false, prop.Name, indexType) {
				s.retirePropertyOverlayLocked(prop.Name, indexType)
			}
		}
	}
}

func (s *Shard) withMigratedIndexes(prop *models.Property) *models.Property {
	o, ok := s.SnapshotPropertyOverlay([]string{prop.Name})[prop.Name]
	if !ok || (!o.ForceFilterable && !o.ForceSearchable && !o.ForceRangeable) {
		return prop
	}
	on := true
	effective := *prop
	if o.ForceFilterable {
		effective.IndexFilterable = &on
	}
	if o.ForceSearchable {
		effective.IndexSearchable = &on
	}
	if o.ForceRangeable {
		effective.IndexRangeFilters = &on
	}
	return &effective
}
