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

// Package conditional defines the shared outcome-enum constants for
// conditional write operations. These constants are the canonical domain-layer
// representation; gRPC and REST layers map to and from these values.
package conditional

// Outcome is the per-object result of a conditional write operation.
type Outcome int32

const (
	// OutcomeUnspecified is the zero value; indicates outcome was not set.
	OutcomeUnspecified Outcome = 0
	// OutcomeInserted means the object was newly created.
	OutcomeInserted Outcome = 1
	// OutcomeSkipped means the object was not written because it already
	// exists (insert_if_not_exists condition on a UUID that is taken).
	OutcomeSkipped Outcome = 2
	// OutcomeUpdated means the object was updated successfully.
	OutcomeUpdated Outcome = 3
	// OutcomeConditionFailed means the field-predicate condition was not
	// satisfied; the write was not performed.
	OutcomeConditionFailed Outcome = 4
	// OutcomeVersionMismatch means the stored version did not match the
	// caller's expected_version; the write was not performed.
	OutcomeVersionMismatch Outcome = 5
	// OutcomeNotFound means no object with the given UUID exists when a
	// version-CAS or field-predicate update was attempted.
	OutcomeNotFound Outcome = 6
)

// String returns a human-readable name for o. Useful for logging and error
// messages without importing the gRPC generated package.
func (o Outcome) String() string {
	switch o {
	case OutcomeInserted:
		return "inserted"
	case OutcomeSkipped:
		return "skipped"
	case OutcomeUpdated:
		return "updated"
	case OutcomeConditionFailed:
		return "condition_failed"
	case OutcomeVersionMismatch:
		return "version_mismatch"
	case OutcomeNotFound:
		return "not_found"
	default:
		return "unspecified"
	}
}

// IsFailure reports whether this outcome represents a condition that prevented
// the write from completing (i.e. the object was NOT written).
func (o Outcome) IsFailure() bool {
	switch o {
	case OutcomeSkipped, OutcomeConditionFailed, OutcomeVersionMismatch, OutcomeNotFound:
		return true
	default:
		return false
	}
}

// IsSuccess reports whether the write was actually performed.
func (o Outcome) IsSuccess() bool {
	return o == OutcomeInserted || o == OutcomeUpdated
}
