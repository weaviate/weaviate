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

// The schema already has this index, so the message must not send the user
// to change it: only fixing this node's migration state brings it back.
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
		"but this shard does not serve yet: a migration on it has not been promoted here, or a migration " +
		"record under the shard's .migrations directory cannot be read. Fix or remove an unreadable record, " +
		"then reload the shard"
	missingSearchableFormat = "Searching by property '%s' requires inverted index. " +
		"Is `indexSearchable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	missingFilterableMetaCountFormat = "Searching by property '%s' count requires inverted index. " +
		"Is `indexFilterable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
)
