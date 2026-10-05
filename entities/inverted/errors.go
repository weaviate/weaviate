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

import "fmt"

type MissingIndexError struct {
	format string
	args   []any
}

func NewMissingFilterableIndexError(propName string) error {
	return MissingIndexError{missingFilterableFormat, []any{propName, propName}}
}

// The schema already has this index, so the message must not send the user to change it.
func NewWithheldRangeableIndexError(propName string) error {
	return MissingIndexError{withheldRangeableFormat, []any{propName}}
}

func NewMissingSearchableIndexError(propName string) error {
	return MissingIndexError{missingSearchableFormat, []any{propName, propName}}
}

func NewMissingFilterableMetaCountIndexError(propName string) error {
	return MissingIndexError{missingFilterableMetaCountFormat, []any{propName, propName}}
}

func (e MissingIndexError) Error() string {
	return fmt.Sprintf(e.format, e.args...)
}

const (
	missingFilterableFormat = "Filtering by property '%s' requires inverted index. " +
		"Is `indexFilterable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	withheldRangeableFormat = "Filtering by property '%s' needs its range index, which the schema has " +
		"but this shard does not serve yet. That is so while a migration on it is not promoted on this " +
		"shard, and after the shard's migration record store (under its .migrations directory) reported a " +
		"fault when the shard loaded: the shard's log from that load names the fault and what clears it. " +
		"Clear it, then reload the shard"
	missingSearchableFormat = "Searching by property '%s' requires inverted index. " +
		"Is `indexSearchable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	missingFilterableMetaCountFormat = "Searching by property '%s' count requires inverted index. " +
		"Is `indexFilterable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
)
