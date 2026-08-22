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

// migrationEffectSatisfied reports whether the locally applied schema shows
// this migration's effect. It is not one rule: five of the six type groups
// commit their effect only on whole-task success, so for them a visible flag
// proves the task committed. The rangeable row does not, and carries its own
// argument below.
//
// A property that is gone from the class counts as satisfied. A property
// deleted after promotion takes the effect's carrier with it, the predicate
// would otherwise be permanently false, and no successor can ever cover a
// property that no longer exists.
//
// A new migration type lands here with its own row and its own argument.
func migrationEffectSatisfied(class *models.Class, subject MigrationSubject) bool {
	satisfied, _ := migrationEffectStatus(class, subject)
	return satisfied
}

// migrationEffectStatus is [migrationEffectSatisfied] plus what it refused on,
// for the caller that has to tell an operator which property is still waiting.
// The property list is empty when the schema shows nothing per-property about
// the refusal — a class-wide flag, or a subject naming no property at all.
func migrationEffectStatus(class *models.Class, subject MigrationSubject) (satisfied bool, missing []string) {
	switch subject.MigrationType {
	case ReindexTypeRepairFilterable, ReindexTypeRebuildSearchable:
		// Post-condition equals pre-condition: there is no flag to read, so
		// the task-status rows carry the disposition alone.
		return true, nil
	case ReindexTypeChangeAlgorithm:
		if class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND {
			return true, nil
		}
	default:
		// Every other type's effect is per property, and is read below.
	}

	if len(subject.Properties) == 0 {
		return false, nil
	}

	byName := make(map[string]*models.Property, len(class.Properties))
	for _, prop := range class.Properties {
		byName[prop.Name] = prop
	}

	for _, name := range subject.Properties {
		prop, present := byName[name]
		if !present {
			continue
		}
		if !migrationPropertyEffectVisible(subject, prop) {
			missing = append(missing, name)
		}
	}
	return len(missing) == 0, missing
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
	case ReindexTypeRepairFilterable, ReindexTypeRebuildSearchable:
		return true
	default:
		return false
	}
}
