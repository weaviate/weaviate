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

// The three questions every passive reader is allowed to ask a record. A
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
