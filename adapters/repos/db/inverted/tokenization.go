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
// AND the searchable bucket for a property as one consistent snapshot (read
// under the per-shard tokenization-overlay lock), with the bucket PINNED for
// the full query; the caller MUST invoke release exactly once (typically
// deferred). The snapshot prevents a query from mixing the post-swap bucket
// with the pre-swap tokenization (or vice versa) during a retokenization's
// FINALIZING window; the pin makes the migration's oldBucket.Shutdown drain
// the in-flight query before freeing mmap'd segments.
//
// The returned bucket may be nil (no searchable bucket for the prop); the
// release is then a no-op but must still be called. Production wires this to
// Shard.PinTokenizationAndSearchableBucket; unset, callers fall back to the
// independent TokenizationResolver + GetBucket pair (correct when no
// retokenization can race the query).
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
