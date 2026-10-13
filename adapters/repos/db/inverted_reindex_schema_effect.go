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
