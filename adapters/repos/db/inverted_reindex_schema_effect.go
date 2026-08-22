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
// effect. Three answers, because "not shown yet" and "nothing here can ever
// show it" license different things: the first is a migration still waiting
// on a schema change, the second is a migration whose outcome no schema read
// decides at all.
type migrationEffect int

const (
	// migrationEffectPending: the schema can carry this effect and does not.
	migrationEffectPending migrationEffect = iota
	// migrationEffectVisible: the schema carries it.
	migrationEffectVisible
	// migrationEffectUnobservable: no schema read can settle this migration.
	// Two migration types have no flag at all, and a subject whose properties
	// have all been deleted took its flags away with them.
	migrationEffectUnobservable
)

// migrationEffectStatus reads a migration's effect out of the locally applied
// schema, and names the properties it is still waiting on for the caller that
// has to tell an operator which one. That list is empty unless the answer is
// pending.
//
// It is not one rule: five of the six type groups commit their effect only on
// whole-task success, so for them a visible flag proves the task committed.
// The rangeable row does not, and carries its own argument below.
//
// A new migration type lands here with its own row and its own argument.
func migrationEffectStatus(class *models.Class, subject MigrationSubject) (migrationEffect, []string) {
	// classWideCarrier is a flag that survives the deletion of every property
	// the subject names, so running out of properties below does not mean the
	// schema has stopped answering.
	classWideCarrier := false

	switch subject.MigrationType {
	case ReindexTypeRepairFilterable, ReindexTypeRebuildSearchable:
		// Post-condition equals pre-condition, so there is no flag to read on
		// the class or on any property.
		return migrationEffectUnobservable, nil
	case ReindexTypeChangeAlgorithm:
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return migrationEffectVisible, nil
		}
		classWideCarrier = true
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
			// carrier with it, so it is evidence in neither direction: it
			// cannot refuse the answer and it cannot supply one.
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
	case classWideCarrier:
		// Every property is gone, but the class flag is still there and still
		// says the effect never landed. That is the schema answering, not the
		// schema falling silent.
		return migrationEffectPending, nil
	default:
		return migrationEffectUnobservable, nil
	}
}

// migrationEffectConfirmsCommit reports that the schema is positive evidence
// this migration's task committed, which only a visible effect is. An effect
// nothing in the schema can show proves nothing, and a caller that reads it
// as proof promotes a migration an operator cancelled — permanently, and only
// on the replicas that took this path.
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
		// This flag is committed by the FIRST shard's swap, unconditionally,
		// so it is never proof that THIS shard swapped. Using it as a
		// disposition input is still sound, and it is the fallback that makes
		// it so, not the flag: the flag is monotonic and never reverted, and a
		// shard that has not swapped serves range queries from the filterable
		// bucket, so every mixed state stays query-correct.
		return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
	default:
		// The two flagless types never reach here: [migrationEffectStatus]
		// answers for them before any property is read.
		return false
	}
}
