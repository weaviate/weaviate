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

package inverted

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

// SearchableBucketPinningResolver returns the active query-time tokenization
// AND the searchable bucket pointer for a property, read as one consistent
// snapshot under the per-shard tokenization-overlay lock, and PINS the
// returned bucket for the full duration of the query, handing back a release
// function the caller MUST invoke exactly once (typically deferred).
//
// The consistent snapshot closes the FINALIZING-window race of a field→word
// searchable retokenization at its root: resolving the tokenization and
// fetching the bucket separately (TokenizationResolver + a standalone
// store.Bucket call) lets a query observe the post-swap bucket with the
// pre-swap tokenization (or vice versa) for the brief gap between the two
// writes. Pairing the two reads under the shard's overlay RLock — matched by
// the write side setting both under the overlay write lock — guarantees the
// query sees a consistent (bucket, tokenization) pair.
//
// The pin keeps that bucket pointer alive across the rest of the migration:
// after flipping the pointer (store.SwapBucketPointer) it shuts the displaced
// old bucket down (oldBucket.Shutdown frees its mmap'd segments). A BM25
// query reads the searchable bucket at prop discovery AND re-reads it at
// lookup (createTerm / createBlockTerm); pinning here and threading the
// pinned pointer to lookup means the query uses ONE bucket for its whole
// duration, and the migration's Shutdown drains the in-flight pin before
// freeing the mmap.
//
// The returned bucket may be nil (property has no searchable bucket); the
// release is then a no-op but MUST still be called. Production wires this to
// Shard.PinTokenizationAndSearchableBucket. When unset, callers fall back to
// the independent TokenizationResolver + GetBucket pair (correct for any
// caller with no in-flight retokenization, since no swap+Shutdown can race
// the lookup).
type SearchableBucketPinningResolver func(propName, schemaTokenization string) (tokenization string, bucket *lsmkv.Bucket, release func())

// TokenizationResolver returns the active query-time tokenization for a
// property given the schema-stored value. Used by query paths
// (BM25Searcher, Searcher, aggregators) to consult a per-shard
// tokenization overlay before falling back to the schema-stored
// value on the property.
//
// The overlay is what closes the FINALIZING-window misalignment of a
// change-tokenization migration: on each replica the bucket pointer
// flips to NEW-tokenized data before the cluster-wide schema flip
// commits via RAFT, so queries that arrive in that window must tokenize
// their input against the NEW value (matching the bucket content)
// rather than the still-OLD schema value.
//
// Nil resolver means "no overlay configured" — typical for tests and
// for callers that have no in-flight migration. Use
// [ResolveTokenization] to handle the nil case at call sites without
// boilerplate.
type TokenizationResolver func(propName, schemaTokenization string) string

// ResolveTokenization applies r to (propName, schemaTokenization) if r
// is non-nil, otherwise returns schemaTokenization unchanged.
//
// Intended to replace direct reads of `prop.Tokenization` on the query
// path so the same line works whether or not a per-shard overlay is
// configured. The expected substitution at call sites is:
//
//	// Before:
//	tok := prop.Tokenization
//	// After:
//	tok := ResolveTokenization(b.tokResolver, prop.Name, prop.Tokenization)
func ResolveTokenization(r TokenizationResolver, propName, schemaTokenization string) string {
	if r == nil {
		return schemaTokenization
	}
	return r(propName, schemaTokenization)
}
