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

import "slices"

// The four questions every passive reader is allowed to ask a record. A
// reader that needs something finer is the engine, discovery or
// reconciliation talking to itself, and switches on the variant instead.
//
// Composition stays out of call sites: the one rule that needs two answers
// (DataCommitted and not PointerSwapped means the staged data is
// discardable) lives inside reconciliation's cancel edge.
type migrationRecordQuestions interface {
	// DataCommitted reports whether this migration's data is complete in the
	// ingest buckets. False means its directories are staging and a sweep may
	// reclaim them; true means they are, or are about to be, the live data.
	DataCommitted() bool

	// PointerSwapped reports whether the flip decision is durable. From here
	// the migration is irreversible: the new buckets may hold acknowledged
	// writes the old copy never received.
	PointerSwapped() bool

	// LiveDataAt names the directory serving property prop right now. Empty
	// means this record has nothing to say about that property.
	LiveDataAt(prop string) string

	// OwnsBucket reports whether dir is one this migration created. Directory
	// names are opaque, so an unattributed directory can never be reclaimed.
	OwnsBucket(dir string) bool
}

// OwnsBucket is a subject fact, not a state fact: a migration owns the
// directories it created from the moment it creates them until they are gone.
// The canonical directory is deliberately not among them — it predates the
// migration and outlives it.
func (b migrationRecordBase) OwnsBucket(dir string) bool {
	if dir == "" {
		return false
	}
	if slices.Contains(b.subject.SidecarDirs, dir) {
		return true
	}
	for _, staged := range b.subject.StagedDirs {
		if staged == dir {
			return true
		}
	}
	return false
}

func (r MigrationRecordIterating) DataCommitted() bool { return false }
func (r MigrationRecordIterated) DataCommitted() bool  { return false }
func (r MigrationRecordMerged) DataCommitted() bool    { return true }
func (r MigrationRecordSwapped) DataCommitted() bool   { return true }
func (r MigrationRecordPromoted) DataCommitted() bool  { return true }

func (r MigrationRecordIterating) PointerSwapped() bool { return false }
func (r MigrationRecordIterated) PointerSwapped() bool  { return false }
func (r MigrationRecordMerged) PointerSwapped() bool    { return false }
func (r MigrationRecordSwapped) PointerSwapped() bool   { return true }
func (r MigrationRecordPromoted) PointerSwapped() bool  { return true }

// Before the flip the canonical bucket is still primary: every acknowledged
// write lands there natively and the staged copy is only a mirror.
func (r MigrationRecordIterating) LiveDataAt(prop string) string {
	return r.subject.CanonicalDirs[prop]
}

func (r MigrationRecordIterated) LiveDataAt(prop string) string {
	return r.subject.CanonicalDirs[prop]
}

func (r MigrationRecordMerged) LiveDataAt(prop string) string {
	return r.subject.CanonicalDirs[prop]
}

// A flipped property is served from the staged directory until promotion
// renames it onto the canonical name; an unflipped one is still canonical.
func (r MigrationRecordSwapped) LiveDataAt(prop string) string {
	if !r.flipCommitted(prop) {
		return r.subject.CanonicalDirs[prop]
	}
	return r.subject.StagedDirs[prop]
}

// Promotion has already put the data at the canonical name.
func (r MigrationRecordPromoted) LiveDataAt(prop string) string {
	return r.subject.CanonicalDirs[prop]
}

// flipCommitted answers for the in-process window between the first and last
// pointer flip, where the durable record already says Swapped but only some
// properties have actually flipped. Outside that window — including for every
// record read back from disk — the recorded flip set is the whole answer,
// because the flips themselves are volatile and a load re-does all of them.
func (r MigrationRecordSwapped) flipCommitted(prop string) bool {
	if !slices.Contains(r.flipped, prop) {
		return false
	}
	if r.runtimeFlipped == nil {
		return true
	}
	_, done := r.runtimeFlipped[prop]
	return done
}

// EnterFlipWindow marks the record as mid-flip: from here LiveDataAt reports
// a property as flipped only once WithPropertyFlipped has said so. The window
// is in-memory only and is never serialized, because a partial flip set is
// not a state a load can ever observe.
func (r MigrationRecordSwapped) EnterFlipWindow() MigrationRecordSwapped {
	r.runtimeFlipped = map[string]struct{}{}
	return r
}

// WithPropertyFlipped returns a copy that reports prop as flipped. Copying
// rather than mutating is what lets the store publish each step without a
// reader ever seeing a half-written map.
func (r MigrationRecordSwapped) WithPropertyFlipped(prop string) MigrationRecordSwapped {
	flipped := make(map[string]struct{}, len(r.runtimeFlipped)+1)
	for existing := range r.runtimeFlipped {
		flipped[existing] = struct{}{}
	}
	flipped[prop] = struct{}{}
	r.runtimeFlipped = flipped
	return r
}

// SetInMemory publishes rec without touching disk. Only the flip window uses
// it: the rule forbidding I/O between the first and last pointer flip is what
// makes a memory-only publish the only option there.
func (s *MigrationRecordStore) SetInMemory(rec MigrationRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.Subject().Key] = rec
}
