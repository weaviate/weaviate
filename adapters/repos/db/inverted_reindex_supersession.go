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
	"context"
	"slices"

	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type migrationDisplacer interface {
	displacedFor(dir string) (string, bool)
	DisplacedDir(prop string) (string, bool)
}

func (f migrationFlipBlock) displacedFor(dir string) (string, bool) {
	if dir == "" {
		return "", false
	}
	for prop, displaced := range f.displacedDirs {
		if displaced == dir {
			return prop, true
		}
	}
	return "", false
}

// The bar is Swapped, not Merged: a staged-but-undecided successor may still
// be cancelled, and treating it as settled would withhold a promotion.
func migrationPropertySuperseded(all []MigrationRecord, subject MigrationSubject, prop string) bool {
	return migrationPropertySupersededExcept(all, subject, prop, MigrationRecordKey{})
}

// except names a record the caller is about to remove, so the answer is what
// supersession would read as once that record is gone.
func migrationPropertySupersededExcept(all []MigrationRecord, subject MigrationSubject,
	prop string, except MigrationRecordKey,
) bool {
	canonical := subject.Props[prop].Canonical
	if canonical == "" {
		return false
	}
	for _, other := range all {
		if other.Subject().Key == except {
			continue
		}
		if migrationSupersedes(other, subject) && other.Subject().Props[prop].Canonical == canonical {
			return true
		}
	}
	return false
}

// Removing a record while it is the only thing superseding an older one
// un-supersedes that older record, and the next load then promotes the older
// record's staged data over the directory this record's data lives in.
func migrationSoleSupersessorOf(all []MigrationRecord, subject MigrationSubject) (MigrationRecordKey, bool) {
	for _, other := range all {
		older := other.Subject()
		if older.Key == subject.Key {
			continue
		}
		for _, prop := range older.Properties() {
			if migrationPropertySuperseded(all, older, prop) &&
				!migrationPropertySupersededExcept(all, older, prop, subject.Key) {
				return older.Key, true
			}
		}
	}
	return MigrationRecordKey{}, false
}

func migrationSupersedes(candidate MigrationRecord, subject MigrationSubject) bool {
	key := candidate.Subject().Key
	return key != subject.Key && key.TaskVersion > subject.Key.TaskVersion && candidate.FlipDecided()
}

type migrationDirRole string

const (
	migrationRoleCanonical migrationDirRole = "canonical directory"
	migrationRoleStaged    migrationDirRole = "staged directory"
	migrationRoleSidecar   migrationDirRole = "sidecar directory"
)

var migrationLiveDataRoles = migrationRolesWithShape(migrationShapeSidecar)

var migrationReclaimBlockingRoles = append(slices.Clone(migrationLiveDataRoles), migrationRoleCanonical)

// [validateOneOwnerPerDirectory] can't catch this: it sees one record at a
// time, so two individually valid records can still collide on live data.
func migrationDirHeldByAnotherRecord(all []MigrationRecord, subject MigrationSubject,
	dir string, roles []migrationDirRole,
) (MigrationRecordKey, migrationDirRole, bool) {
	for _, other := range all {
		key := other.Subject().Key
		if key == subject.Key {
			continue
		}
		for _, role := range roles {
			for _, held := range migrationDirsInRole(other.Subject(), role) {
				if held == dir {
					return key, role, true
				}
			}
		}
	}
	return MigrationRecordKey{}, "", false
}

func migrationDirsInRole(subject MigrationSubject, role migrationDirRole) map[string]string {
	for _, group := range migrationHandleGroups {
		if group.field == string(role) && group.dirs != nil {
			return group.dirs(migrationRecordEnvelope{Subject: subject})
		}
	}
	return nil
}

// The canonical directory is the property's live bucket, never one of these.
func migrationOwnCopyDirs(subject MigrationSubject, prop string) []string {
	own := subject.Props[prop]
	dirs := make([]string, 0, 2)
	for _, dir := range []string{own.Staged, own.Sidecar} {
		if dir != "" {
			dirs = append(dirs, dir)
		}
	}
	return dirs
}

func migrationDirClaimedAsDisplaced(all []MigrationRecord, subject MigrationSubject, dir string) bool {
	for _, other := range all {
		if !migrationSupersedes(other, subject) {
			continue
		}
		displacer, ok := other.(migrationDisplacer)
		if !ok {
			continue
		}
		claimedFor, ok := displacer.displacedFor(dir)
		if _, named := other.Subject().Props[claimedFor]; !ok || !named {
			continue
		}
		if migrationPropertySuperseded(all, other.Subject(), claimedFor) {
			continue
		}
		return true
	}
	return false
}

func (r *migrationReconciler) RetireSuperseded(ctx context.Context) {
	if len(r.store.Unreadable()) > 0 {
		return
	}
	for _, rec := range r.store.Records() {
		if ctx.Err() != nil {
			return
		}
		subject := rec.Subject()
		if len(subject.Props) == 0 {
			continue
		}

		// Read at this record's turn, not once for the whole sweep: a record
		// retired earlier here answers for nothing any more, and a list taken
		// before the sweep began still counts its claims.
		all := r.store.Records()
		superseded := supersededProperties(all, subject)
		if len(superseded) == 0 {
			continue
		}
		if !migrationRetirable(rec, superseded) {
			continue
		}

		_ = r.withSealedUnit(subject, "its retirement", func() error {
			r.retireOneSealed(ctx, all, subject, superseded)
			return nil
		})
	}
}

// An unflipped record may retire too, but only once every property is
// superseded — otherwise a superseded-but-unflipped record wedges forever,
// its tracker dragging cold tenants into hydration.
func migrationRetirable(rec MigrationRecord, superseded []string) bool {
	if rec.StagedDataComplete() {
		return true
	}
	return !rec.FlipDecided() && len(superseded) == len(rec.Subject().Props)
}

func supersededProperties(all []MigrationRecord, subject MigrationSubject) []string {
	var out []string
	for _, prop := range subject.Properties() {
		if migrationPropertySuperseded(all, subject, prop) {
			out = append(out, prop)
		}
	}
	return out
}

func (r *migrationReconciler) retireOneSealed(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, superseded []string,
) {
	// One line per record, not one per property: retirement runs on every shard
	// load and a record names as many properties as its request asked for.
	retiring := errorcompounder.New()
	for _, prop := range superseded {
		retiring.Add(r.retireProperty(ctx, all, subject, prop))
	}
	if err := retiring.ToErrorLimited(maxReportedErrors); err != nil {
		r.logger.WithField("record", subject.Key.String()).
			WithField("properties_superseded", len(superseded)).
			Errorf("retire the superseded properties of a migration: %v", err)
		return
	}
	if len(superseded) != len(subject.Props) {
		return
	}
	if err := r.reclaimRecordAndDirs(ctx, subject); err != nil {
		r.logger.WithField("record", subject.Key.String()).
			Errorf("reclaim a superseded migration: %v", err)
		return
	}
	r.logger.WithField("record", subject.Key.String()).
		WithField("properties_retired", len(superseded)).
		Info("a newer migration took over every property of this one, so its record and directories are reclaimed")
}

// Closes after deciding, not before: a bucket this pass shuts down is
// deregistered until the shard next loads, so closing one whose directory is
// then left in place stops that data serving with nothing to reopen it.
func (r *migrationReconciler) retireProperty(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, prop string,
) error {
	if r.deps.Mirror != nil {
		r.deps.Mirror.DisarmMigrationMirror(subject.Key, prop)
	}
	// Every directory holding this record's own copy of the property, not the
	// staged one alone: the record stops answering for the property here, and a
	// sidecar left behind is data at a name nothing attributes any more.
	for _, dir := range migrationOwnCopyDirs(subject, prop) {
		if migrationRetirementLeavesStagedDir(all, subject, dir) {
			continue
		}
		if err := r.closeStagedBuckets(ctx, dir); err != nil {
			return err
		}
		if err := r.removeDir(r.lsmPath, dir, "a directory of a superseded migration"); err != nil {
			return err
		}
	}
	return nil
}

func migrationRetirementLeavesStagedDir(all []MigrationRecord,
	subject MigrationSubject, dir string,
) bool {
	if dir == "" || migrationDirClaimedAsDisplaced(all, subject, dir) {
		return true
	}
	_, _, held := migrationDirHeldByAnotherRecord(all, subject, dir, migrationReclaimBlockingRoles)
	return held
}
