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

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
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

// migrationOverridesSchema reports whether rec, not the schema, answers for
// what it migrated: from its flip decision until its task finishes or leaves the list.
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

func (s *Shard) restoreMigratedOverlays(class *models.Class) {
	if s.migrationRecords == nil || class == nil {
		return
	}
	tasks, readable := s.migrations().LocalTasks()
	all := s.migrationRecords.Records()
	records := slices.SortedFunc(slices.Values(all), func(a, b MigrationRecord) int {
		return cmp.Compare(a.Subject().Key.TaskVersion, b.Subject().Key.TaskVersion)
	})
	for _, rec := range records {
		subject := rec.Subject()
		if !migrationOverridesSchema(rec, tasks, readable) {
			continue
		}
		payload := subject.reindexPayload()
		built, err := buildMigrationTasks(s.index.logger, nil, subject.Key.TaskVersion, &payload,
			func(string, string) bool { return true })
		if err != nil {
			s.index.logger.WithField("record", subject.Key.String()).
				Errorf("cannot rebuild the index settings this shard migrated to: %v", err)
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

func (s *Shard) migratedIndexStands(propName, indexType string) bool {
	if s.migrationRecords == nil {
		return false
	}
	tasks, readable := s.migrations().LocalTasks()
	for _, rec := range s.migrationRecords.Records() {
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
