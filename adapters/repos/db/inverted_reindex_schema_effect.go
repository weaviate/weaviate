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

import "github.com/weaviate/weaviate/entities/models"

// migrationEffect is what the locally applied schema says about a migration's
// effect. "Not shown yet" (waiting on a schema change) and "never shown"
// (nothing decides this by schema) license different actions.
type migrationEffect int

const (
	// migrationEffectPending: the schema can carry this effect and does not.
	migrationEffectPending migrationEffect = iota
	// migrationEffectVisible: the schema carries it.
	migrationEffectVisible
	// migrationEffectUnobservable: no schema read can decide this migration.
	// Two migration types have no flag at all, and a subject whose properties
	// have all been deleted took its flags away with them.
	migrationEffectUnobservable
)

// migrationEffectStatus reads a migration's effect out of the locally applied
// schema, naming properties still waiting on it. Five of six type groups
// commit their effect only on whole-task success, so a visible flag proves
// commit; the rangeable row does not (own argument below).
func migrationEffectStatus(class *models.Class, subject MigrationSubject) (migrationEffect, []string) {
	switch subject.MigrationType {
	case ReindexTypeRepairFilterable, ReindexTypeRebuildSearchable:
		// Post-condition equals pre-condition, so there is no flag to read on
		// the class or on any property.
		return migrationEffectUnobservable, nil
	case ReindexTypeChangeAlgorithm:
		// Positive evidence only: the cutover skips this flip while another
		// searchable property in the class is still on WAND, so it can stay
		// false for a finished migration; the per-property stamp always lands.
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return migrationEffectVisible, nil
		}
	default:
		// Every other type's effect is per property, and is read below.
	}

	if len(subject.Properties) == 0 {
		return migrationEffectPending, nil
	}

	byName := make(map[string]*models.Property, len(class.Properties))
	for _, prop := range class.Properties {
		byName[prop.Name] = prop
	}

	var missing []string
	observable := 0
	for _, name := range subject.Properties {
		prop, present := byName[name]
		if !present {
			// A property deleted after the migration took the effect's only
			// evidence with it: it cannot refuse the answer or supply one.
			continue
		}
		observable++
		if !migrationPropertyEffectVisible(subject, prop) {
			missing = append(missing, name)
		}
	}
	switch {
	case len(missing) > 0:
		return migrationEffectPending, missing
	case observable > 0:
		return migrationEffectVisible, nil
	default:
		// Every property that could still answer is gone. The class-wide
		// blockmax flag is not evidence either: it is skipped outright while a
		// sibling property is still on WAND, so its being unset decides
		// nothing.
		return migrationEffectUnobservable, nil
	}
}

// migrationEffectConfirmsCommit reports that the schema is positive evidence
// this migration's task committed — true only for a visible effect. Reading
// an unobservable effect as proof would permanently promote a cancelled
// migration.
func migrationEffectConfirmsCommit(class *models.Class, subject MigrationSubject) bool {
	effect, _ := migrationEffectStatus(class, subject)
	return effect == migrationEffectVisible
}

func migrationPropertyEffectVisible(subject MigrationSubject, prop *models.Property) bool {
	switch subject.MigrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		return prop.Tokenization == subject.TargetTokenization
	case ReindexTypeEnableFilterable:
		return prop.IndexFilterable != nil && *prop.IndexFilterable
	case ReindexTypeEnableSearchable:
		return prop.IndexSearchable != nil && *prop.IndexSearchable &&
			prop.Tokenization == subject.TargetTokenization &&
			prop.SearchableBlockmax != nil && *prop.SearchableBlockmax
	case ReindexTypeChangeAlgorithm:
		return prop.SearchableBlockmax != nil && *prop.SearchableBlockmax
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		// This flag commits on the FIRST shard's swap, unconditionally, so it is
		// never proof THIS shard swapped. Still sound: it is monotonic and never
		// reverted, and an unswapped shard serves range queries from filterable.
		return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
	default:
		// The two flagless types never reach here: [migrationEffectStatus]
		// decides them before any property is read.
		return false
	}
}
