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

// migrationEnablesIndexType names the index a migration turns on. Until its
// flip lands, the schema keeps that index off while migrated shards serve it.
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
// what it migrated: from its flip decision until its owning task finishes or
// is gone, either of which means the flip landed. A task list not readable
// yet keeps the record answering, which a migration in flight needs.
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

// restoreMigratedOverlays re-installs the overlay a swap left in memory
// for a migration promoted here whose flip may not have landed (a sibling
// tenant failed, or the flip is in flight); without it, reload would read
// and write those buckets under the schema's stale settings.
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
			// Not the unmirrored flag: the load refuses to promote a stale copy over
			// live data, so whatever it did promote is the only data there is.
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

// migratedIndexStands reports whether one of this shard's own migrations
// turned the index on and still answers for it, so the schema's index-off
// sweeps must leave its bucket alone.
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
		// A promotion the load refused leaves an unmirrored copy stale; one it ran made it the data.
		if swapped, isSwapped := rec.(MigrationRecordSwapped); subject.Unmirrored && isSwapped &&
			swapped.PromotionOf(propName) != migrationPromotionFinished {
			continue
		}
		return true
	}
	return false
}

// withMigratedIndexes returns prop with every index a restored overlay forces
// on, so the bucket a migration enabled is opened even though the schema
// still has it off.
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
