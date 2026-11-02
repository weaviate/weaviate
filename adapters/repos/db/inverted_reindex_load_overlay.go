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

// restoreMigratedOverlays re-installs, from the shard's own records, what a
// swap put in memory: a shard whose migration is promoted but whose flip has
// not landed (another tenant failed, or the flip is still in flight) holds
// buckets the schema does not describe. Without this a reload reads and
// writes them with the schema's tokenization and index flags.
func (s *Shard) restoreMigratedOverlays(class *models.Class) {
	if s.migrationRecords == nil || class == nil {
		return
	}
	all := s.migrationRecords.Records()
	records := slices.SortedFunc(slices.Values(all), func(a, b MigrationRecord) int {
		return cmp.Compare(a.Subject().Key.TaskVersion, b.Subject().Key.TaskVersion)
	})
	for _, rec := range records {
		subject := rec.Subject()
		// An unmirrored record's staged copy missed writes, so it never becomes the data.
		if !rec.FlipDecided() || subject.Unmirrored {
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
// turned the index on: a swap the schema does not show yet, whose bucket the
// schema's index-off sweeps must leave alone.
func (s *Shard) migratedIndexStands(propName, indexType string) bool {
	if s.migrationRecords == nil {
		return false
	}
	for _, rec := range s.migrationRecords.Records() {
		subject := rec.Subject()
		enabled, ok := migrationEnablesIndexType(subject.MigrationType)
		if ok && enabled == indexType && rec.FlipDecided() && !subject.Unmirrored {
			if _, covers := subject.Props[propName]; covers {
				return true
			}
		}
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
