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

// PredicateOperator enumerates the comparison operators supported by Predicate.
// For Phase-3 v1 only EqOperator is implemented; other values are reserved for
// future minor releases and are rejected with a clear error if received.
type PredicateOperator int

const (
	// EqOperator checks that the stored property value equals the expected value.
	// This is the only operator supported in Phase-3 v1.
	EqOperator PredicateOperator = iota
)

// PredicateValueType is the Weaviate schema type of the expected value in a
// Predicate. It must be supplied explicitly so the evaluator knows how to
// deserialize the stored property and compare it, without a schema round-trip
// on the hot write path.
type PredicateValueType int

const (
	// PredicateValueText treats Value as a UTF-8 string. Comparison is exact,
	// case-sensitive (matching Weaviate's equality semantics for text properties).
	PredicateValueText PredicateValueType = iota
	// PredicateValueInt treats Value as an int64. The stored property must be
	// a numeric value that can be compared as an integer.
	PredicateValueInt
	// PredicateValueNumber treats Value as a float64. IEEE-754 equality applies;
	// callers using this type for business logic should prefer PredicateValueText
	// or PredicateValueInt to avoid float precision surprises.
	PredicateValueNumber
	// PredicateValueBool treats Value as a boolean.
	PredicateValueBool
	// PredicateValueDate treats Value as an RFC 3339 timestamp. Both stored and
	// expected values are normalized to UTC before comparison.
	PredicateValueDate
)

// Predicate carries the Phase-3 field-predicate check: "the object's property
// PropertyName must currently equal the typed expected value".
//
// Rules:
//   - If the named property is absent from the stored object, the predicate FAILS
//     (missing != match). The caller receives ErrPreconditionFailed with reason
//     "property <name> not found on stored object".
//   - Multi-value (array) properties are out of scope for Phase-3 v1. Attempting
//     to use a Predicate on an array property returns ErrPreconditionFailed with
//     reason "property <name> is a multi-value array type; field_match on arrays
//     is not supported in v1".
//   - Cross-collection and nested-ref traversal are out of scope.
//   - For Phase-3 v1, Operator must be EqOperator; other values are rejected.
type Predicate struct {
	// PropertyName is the name of the Weaviate property to evaluate.
	// Must be the leaf property name (no dot-path traversal in v1).
	PropertyName string

	// ExpectedValue is the typed expected value. Its Go type must match ValueType:
	//   PredicateValueText   -> string
	//   PredicateValueInt    -> int64
	//   PredicateValueNumber -> float64
	//   PredicateValueBool   -> bool
	//   PredicateValueDate   -> string (RFC 3339, normalized to UTC before compare)
	ExpectedValue interface{}

	// ValueType names the Weaviate schema type of ExpectedValue, so the evaluator
	// knows how to deserialize the stored property without a schema lookup.
	ValueType PredicateValueType

	// Operator is the comparison to apply. Phase-3 v1 supports EqOperator only.
	Operator PredicateOperator
}

// Conditional carries the precondition for a conditional write operation.
// All fields are optional (zero-value = condition kind not active). A caller
// that only uses Phase-1 existence-check fills OnlyIfNotExists or OnlyIfExists
// and leaves IfVersion and UpdateIf nil; Phase-2 callers fill IfVersion;
// Phase-3 callers fill UpdateIf. Multiple non-zero fields are evaluated
// conjunctively, but the REST/gRPC API enforces at most one active condition
// per request in Phase-3 v1.
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
	// only if the stored object's property named Predicate.PropertyName equals
	// Predicate.ExpectedValue under the specified ValueType comparison. A nil
	// pointer means "Phase-3 condition not active."
	UpdateIf *Predicate
}

// IsZero reports whether c carries no active condition (all fields at their
// zero value). An IsZero Conditional is equivalent to an unconditional write.
func (c Conditional) IsZero() bool {
	return !c.OnlyIfNotExists && !c.OnlyIfExists && c.IfVersion == nil && c.UpdateIf == nil
}
