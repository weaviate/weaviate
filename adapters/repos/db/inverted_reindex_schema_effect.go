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
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

type migrationEffect int

const (
	migrationEffectPending migrationEffect = iota
	migrationEffectVisible
	migrationEffectUnobservable
)

// The one place a migration type is answered for: the predicate that reads its
// effect off a property, or nil where the post-condition equals the
// pre-condition and no schema read can confirm or deny it. No default arm, so
// the linter names a tenth type that answers here for neither.
func migrationEffectReader(migrationType ReindexMigrationType) (
	visible func(MigrationSubject, *models.Property) bool, known bool,
) {
	switch migrationType {
	case ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable:
		return func(s MigrationSubject, p *models.Property) bool {
			return propertyTokenizationAtTarget(p, s.TargetTokenization)
		}, true
	case ReindexTypeEnableFilterable:
		return func(_ MigrationSubject, p *models.Property) bool { return propertyFilterableEnabled(p) }, true
	case ReindexTypeEnableSearchable:
		return func(s MigrationSubject, p *models.Property) bool {
			return propertySearchableAtTarget(p, s.TargetTokenization)
		}, true
	case ReindexTypeChangeAlgorithm:
		return func(_ MigrationSubject, p *models.Property) bool { return propertyBlockmaxStamped(p) }, true
	case ReindexTypeEnableRangeable:
		return func(_ MigrationSubject, p *models.Property) bool { return propertyRangeableEnabled(p) }, true
	case ReindexTypeRepairFilterable, ReindexTypeRebuildSearchable, ReindexTypeRepairRangeable:
		return nil, true
	}
	return nil, false
}

func migrationTypeKnown(migrationType ReindexMigrationType) bool {
	_, known := migrationEffectReader(migrationType)
	return known
}

// Asked of the type alone, because migrationEffectStatus reports the same
// answer for a record the applied schema is behind on, which is a different thing.
func migrationEffectIsNeverObservable(migrationType ReindexMigrationType) bool {
	visible, known := migrationEffectReader(migrationType)
	return known && visible == nil
}

func migrationEffectStatus(class *models.Class, subject MigrationSubject) (migrationEffect, []string) {
	if migrationEffectIsNeverObservable(subject.MigrationType) {
		return migrationEffectUnobservable, nil
	}

	if len(subject.Props) == 0 {
		return migrationEffectPending, nil
	}

	byName := make(map[string]*models.Property, len(class.Properties))
	for _, prop := range class.Properties {
		byName[prop.Name] = prop
	}

	var missing []string
	for _, name := range subject.Properties() {
		prop, present := byName[name]
		if !present {
			// Submit rejects a property the class does not hold and Weaviate
			// never removes one, so a schema short of any of them is behind.
			// One missing or all missing is the same condition and reads alike.
			return migrationEffectUnobservable, nil
		}
		if !migrationPropertyEffectVisible(subject, prop) {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return migrationEffectPending, missing
	}
	return migrationEffectVisible, nil
}

// Reading an unobservable effect as proof of commit would permanently
// promote a cancelled migration.
func migrationEffectConfirmsCommit(class *models.Class, subject MigrationSubject) bool {
	effect, _ := migrationEffectStatus(class, subject)
	return effect == migrationEffectVisible
}

// Mirrors the conditions under which the schema writer sets these flags. Where
// the writer narrows and this does not, reconcilePromotedSealed reads the
// effect as pending and never removes the promoted record's tracker directory.
func migrationPropertyEffectVisible(subject MigrationSubject, prop *models.Property) bool {
	visible, _ := migrationEffectReader(subject.MigrationType)
	return visible != nil && visible(subject, prop)
}

// migrationCanonicalIndexFlag reads the schema flag that owns the canonical
// directory this strategy promotes onto, and names the field it read so a
// refusal can say which flag it followed. Sibling of [sourceBucketNameFor]:
// each arm answers for the bucket that one names, so the two stay in step. No
// default arm, so the linter refuses a ninth code that names no flag here.
func migrationCanonicalIndexFlag(code MigrationStrategyCode, prop *models.Property) (*bool, string) {
	switch code {
	case StrategyCodeSearchableMapToBlockmax, StrategyCodeEnableSearchable,
		StrategyCodeRebuildSearchable, StrategyCodeSearchableRetokenize:
		return prop.IndexSearchable, "indexSearchable"
	case StrategyCodeFilterableToRangeable:
		return prop.IndexRangeFilters, "indexRangeFilters"
	case StrategyCodeFilterableRoaringsetRefresh, StrategyCodeFilterableRetokenize,
		StrategyCodeEnableFilterable:
		return prop.IndexFilterable, "indexFilterable"
	}
	return nil, ""
}

// migrationCanonicalSweptBySchema reports whether the load-time sweep would
// delete what a promotion is about to rename onto the canonical name, and why.
//
// The schema flag is the only authority over a canonical property directory:
// [propertyDeleteIndexHelper.ensureBucketsAreRemovedForNonExistentPropertyIndexes]
// deletes one it finds under an index the collection turns off, and it runs
// before the promotion on every load. The migrations that turn an index on run
// with that flag off for their whole duration, so renaming before the
// cluster-wide flip lands puts the rebuilt data exactly where the next load
// deletes it.
//
// Deliberately not [migrationEffectStatus]: that answers whether the effect has
// landed and reads an unset flag as not-enabled, which would defer every
// retokenize and change-algorithm promotion. The hazard is only ever the
// sweep's own rule, an explicit false.
//
// A property the collection does not list is equally unreached by the sweep,
// which walks the collection's own properties, so there is nothing to wait for
// and the rename goes ahead. A collection this node has not applied at all is
// the one thing that cannot be answered: the sweep may well hold a false this
// read cannot see, so that waits for a load that can read it.
func migrationCanonicalSweptBySchema(class *models.Class, subject MigrationSubject,
	prop string,
) (swept bool, why string) {
	if class == nil {
		return true, fmt.Sprintf("property %q: the collection is not in the locally applied schema", prop)
	}
	for _, p := range class.Properties {
		if p == nil || p.Name != prop {
			continue
		}
		flag, field := migrationCanonicalIndexFlag(subject.Key.StrategyCode, p)
		if propertyIndexRemoved(flag) {
			return true, fmt.Sprintf("property %q: the collection sets %s to false", prop, field)
		}
		return false, ""
	}
	return false, ""
}

func propertyTokenizationAtTarget(prop *models.Property, target string) bool {
	return prop.Tokenization == target
}

func propertyFilterableEnabled(prop *models.Property) bool {
	return prop.IndexFilterable != nil && *prop.IndexFilterable
}

func propertySearchableAtTarget(prop *models.Property, target string) bool {
	return prop.IndexSearchable != nil && *prop.IndexSearchable &&
		propertyTokenizationAtTarget(prop, target) &&
		propertyBlockmaxStamped(prop)
}

func propertyBlockmaxStamped(prop *models.Property) bool {
	return prop.SearchableBlockmax != nil && *prop.SearchableBlockmax
}

func propertyRangeableEnabled(prop *models.Property) bool {
	return prop.IndexRangeFilters != nil && *prop.IndexRangeFilters
}
