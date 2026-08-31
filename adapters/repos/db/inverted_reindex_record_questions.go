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

type migrationRecordQuestions interface {
	StagedDataComplete() bool

	// FlipDecided reports whether the flip DECISION is durable. It is
	// written before the first pointer moves, so it never means the flip ran
	// and never means the canonical name holds the migrated data — only
	// MigrationStatePromoted answers that, and it answers "every property is
	// either promoted or superseded", so a superseded property's canonical
	// name holds a successor's data rather than this migration's. From here
	// the migration is irreversible: the new buckets may hold acknowledged
	// writes the old copy never received.
	FlipDecided() bool

	// Both read by the cutover PR: the first before it hands a shard's writes
	// to the staged copy, the second before it opens a bucket at a directory a
	// record may be about to rename or remove.
	IterationComplete() bool
	OwnsBucket(dir string) bool
}

func (b migrationRecordBase) OwnsBucket(dir string) bool {
	return slices.Contains(migrationOwnedDirs(b.subject), dir)
}

func (r MigrationRecordIterating) StagedDataComplete() bool { return false }
func (r MigrationRecordIterated) StagedDataComplete() bool  { return false }
func (r MigrationRecordMerged) StagedDataComplete() bool    { return true }
func (r MigrationRecordSwapped) StagedDataComplete() bool   { return true }
func (r MigrationRecordPromoted) StagedDataComplete() bool  { return true }

func (r MigrationRecordIterating) IterationComplete() bool { return false }
func (r MigrationRecordIterated) IterationComplete() bool  { return true }
func (r MigrationRecordMerged) IterationComplete() bool    { return true }
func (r MigrationRecordSwapped) IterationComplete() bool   { return true }
func (r MigrationRecordPromoted) IterationComplete() bool  { return true }

func (r MigrationRecordIterating) FlipDecided() bool { return false }
func (r MigrationRecordIterated) FlipDecided() bool  { return false }
func (r MigrationRecordMerged) FlipDecided() bool    { return false }
func (r MigrationRecordSwapped) FlipDecided() bool   { return true }
func (r MigrationRecordPromoted) FlipDecided() bool  { return true }
