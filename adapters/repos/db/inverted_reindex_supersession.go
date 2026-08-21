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
	"os"
)

// migrationDisplacer is the part of a flipped record that names what its flip
// pushed aside. Only Swapped and Promoted have one.
type migrationDisplacer interface {
	claimsDisplacedDir(dir string) bool
}

func (f migrationFlipBlock) claimsDisplacedDir(dir string) bool {
	if dir == "" {
		return false
	}
	for _, displaced := range f.displacedDirs {
		if displaced == dir {
			return true
		}
	}
	return false
}

// migrationPropertySuperseded is the supersession predicate. Order comes from
// the generation alone — a consensus-allocated total order already in every
// record — so the relation is closed under any retirement order and any crash:
// removing one record never changes the comparison between two others.
//
// The witness bar is Swapped, not Merged. A successor that has staged but not
// decided may still be cancelled, and treating it as a witness would withhold
// promotion of a record whose own flip already retired the old canonical data,
// leaving the canonical name empty while the successor idles.
//
// "Covers the same property" is decided by comparing the two records' recorded
// canonical directories rather than by classifying index types: a searchable
// and a filterable migration on one property stage into different canonical
// buckets and do not displace each other.
func migrationPropertySuperseded(all []MigrationRecord, subject MigrationSubject, prop string) bool {
	canonical := subject.CanonicalDirs[prop]
	if canonical == "" {
		return false
	}
	for _, other := range all {
		if migrationSupersedes(other, subject) && other.Subject().CanonicalDirs[prop] == canonical {
			return true
		}
	}
	return false
}

func migrationSupersedes(candidate MigrationRecord, subject MigrationSubject) bool {
	key := candidate.Subject().Key
	return key != subject.Key && key.TaskVersion > subject.Key.TaskVersion && candidate.PointerSwapped()
}

// migrationDirClaimedAsDisplaced reports whether a higher-generation record
// that survives this pass has recorded dir as what its own flip displaced. A
// predecessor that flipped but never promoted still holds its live data at a
// staged name, so that name is exactly what a successor displaces — and
// displaced directories have one owner: the record that displaced them.
//
// Deferring matters most where the successor cannot promote: while it sits in
// the preserve-and-surface arm, the directory it displaced is the only copy
// of that property left on disk.
func migrationDirClaimedAsDisplaced(all []MigrationRecord, subject MigrationSubject, dir string) bool {
	for _, other := range all {
		if !migrationSupersedes(other, subject) {
			continue
		}
		displacer, ok := other.(migrationDisplacer)
		if !ok || !displacer.claimsDisplacedDir(dir) {
			continue
		}
		// A claimer that is itself fully superseded is removed by this same
		// pass and will never run its own removal. Deferring to it would
		// strand the directory at a name no surviving record holds, which
		// under opaque naming makes it unattributable and unreclaimable.
		if migrationRecordFullySuperseded(all, other) {
			continue
		}
		return true
	}
	return false
}

// migrationRecordFullySuperseded reports whether every property of rec has a
// higher-generation witness, which is the condition for removing the record
// itself rather than just some of its directories.
func migrationRecordFullySuperseded(all []MigrationRecord, rec MigrationRecord) bool {
	subject := rec.Subject()
	if !rec.DataCommitted() || len(subject.Properties) == 0 {
		return false
	}
	for _, prop := range subject.Properties {
		if !migrationPropertySuperseded(all, subject, prop) {
			return false
		}
	}
	return true
}

// retireSuperseded runs the relation over every committed record. Ascending
// generation is not needed for correctness — the predicate makes any order
// safe — but it leaves the fewest dangling links after a crash and makes the
// outcome deterministic.
func (r *migrationReconciler) retireSuperseded(ctx context.Context, all []MigrationRecord) {
	for _, rec := range all {
		if !rec.DataCommitted() {
			continue
		}
		subject := rec.Subject()
		if len(subject.Properties) == 0 {
			continue
		}

		for _, prop := range subject.Properties {
			if !migrationPropertySuperseded(all, subject, prop) {
				continue
			}
			r.retireProperty(ctx, all, subject, prop)
		}
		if !migrationRecordFullySuperseded(all, rec) {
			continue
		}

		// Every property is gone or has become a successor's responsibility,
		// so the record has nothing left to answer for.
		for _, dir := range subject.SidecarDirs {
			if err := os.RemoveAll(r.path(dir)); err != nil {
				r.logger.WithField("dir", dir).Errorf("remove sidecar directory of a superseded migration: %v", err)
			}
		}
		if err := r.store.Remove(subject.Key); err != nil {
			r.logger.WithField("record", subject.Key.String()).Errorf("remove superseded migration record: %v", err)
		}
	}
}

// retireProperty disarms before it removes. Without that order the directory
// it removes is exactly where the superseded record's still-armed mirror sends
// its next copy, and a failed mirror copy fails the user's write with it —
// which is what makes back-to-back generations safe inside one process.
func (r *migrationReconciler) retireProperty(ctx context.Context, all []MigrationRecord,
	subject MigrationSubject, prop string,
) {
	r.disarmAndClose(ctx, subject.Key, prop)

	dir := subject.StagedDirs[prop]
	if dir == "" || migrationDirClaimedAsDisplaced(all, subject, dir) {
		return
	}
	if err := os.RemoveAll(r.path(dir)); err != nil {
		r.logger.WithField("dir", dir).Errorf("remove staged directory of a superseded migration: %v", err)
	}
}
