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

package storobj

// Predicate is a forward-compatible placeholder for the field-predicate
// condition kind (Phase 3, update_if). The concrete shape is defined in the
// Phase-3 design pass; the field slot exists here so Conditional is
// forward-compatible without an API break.
type Predicate struct {
	// Path is the dotted field path to evaluate (e.g. "status" or "meta.score").
	Path string

	// Value is the expected value, serialised as a string for cross-type
	// compatibility. The Phase-3 design pass will define the supported types
	// and comparison operators.
	Value string
}

// Conditional carries the precondition for a conditional write operation.
// All fields are optional (zero-value = condition kind not active). A caller
// that only uses Phase-1 existence-check fills OnlyIfNotExists or OnlyIfExists
// and leaves IfVersion and UpdateIf nil; Phase-2 callers fill IfVersion;
// Phase-3 callers fill UpdateIf. Multiple non-zero fields are evaluated
// conjunctively.
//
// The three phases correspond to the three CAS primitives in the synthesis:
//   - Phase 1: insert_if_not_exists / OnlyIfNotExists (existence check)
//   - Phase 1b: update_if_exists / OnlyIfExists (existence check, inverse)
//   - Phase 2: _version / if_version=N  (server-managed version CAS)
//   - Phase 3: update_if <field> = <value> (field-predicate update)
type Conditional struct {
	// OnlyIfNotExists requests Phase-1 existence-check semantics: the write
	// succeeds only if no object with the given UUID currently exists.
	// Corresponds to the insert_if_not_exists API primitive.
	OnlyIfNotExists bool

	// OnlyIfExists requests Phase-1b existence-check semantics: the write
	// succeeds only if an object with the given UUID already exists.
	// Corresponds to the update_if_exists API primitive.
	OnlyIfExists bool

	// IfVersion requests Phase-2 version-CAS semantics: the write succeeds only
	// if the stored object's server-managed version equals this value. A nil
	// pointer means "Phase-2 condition not active."
	IfVersion *uint64

	// UpdateIf requests Phase-3 field-predicate semantics: the write succeeds
	// only if the stored object's field at Predicate.Path equals Predicate.Value.
	// A nil pointer means "Phase-3 condition not active."
	UpdateIf *Predicate
}

// IsZero reports whether c carries no active condition (all fields at their
// zero value). An IsZero Conditional is equivalent to an unconditional write.
func (c Conditional) IsZero() bool {
	return !c.OnlyIfNotExists && !c.OnlyIfExists && c.IfVersion == nil && c.UpdateIf == nil
}
