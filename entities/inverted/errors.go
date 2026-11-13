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

import (
	"fmt"
	"strconv"
	"strings"
)

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

// NewNoSearchablePropertiesError is returned when a keyword search does not
// name properties and the collection has nothing a BM25 search can use.
// disabled, when non-empty, are text properties whose searchable index was
// turned off — the usual reason the searchable set is empty.
func NewNoSearchablePropertiesError(className string, disabled []string) error {
	if len(disabled) > 0 {
		return MissingIndexError{noSearchableWithDisabledFormat, []any{className, quoteProps(disabled)}}
	}
	return MissingIndexError{noSearchablePropertiesFormat, []any{className}}
}

// NewSkippedSearchableIndexError is returned when a keyword search does not
// name properties and at least one text property has indexSearchable disabled.
// Other properties may still be searchable. Skipping the disabled ones and
// returning whatever the rest match is a silent miss: the query text may live
// only on the disabled property, and a hybrid search would then look like a
// pure vector search.
func NewSkippedSearchableIndexError(disabled []string) error {
	list := quoteProps(disabled)
	return MissingIndexError{skippedSearchableFormat, []any{list, list}}
}

func quoteProps(props []string) string {
	quoted := make([]string, len(props))
	for i, prop := range props {
		quoted[i] = strconv.Quote(prop)
	}
	switch len(quoted) {
	case 0:
		return ""
	case 1:
		return quoted[0]
	case 2:
		return quoted[0] + " and " + quoted[1]
	default:
		return strings.Join(quoted[:len(quoted)-1], ", ") + ", and " + quoted[len(quoted)-1]
	}
}

func (e MissingIndexError) Error() string {
	return fmt.Sprintf(e.format, e.args...)
}

const (
	missingFilterableFormat = "Filtering by property '%s' requires inverted index. " +
		"Is `indexFilterable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	withheldRangeableFormat = "Filtering by property '%s' needs its range index, which the schema has " +
		"but this shard does not serve yet. While a migration on it runs here, wait for it to finish; if it " +
		"was cancelled, reload the shard. After the shard's migration record store (under its .migrations " +
		"directory) reported a fault when the shard loaded, the shard's log from that load names the fault: " +
		"clear it, then reload the shard"
	missingSearchableFormat = "Searching by property '%s' requires inverted index. " +
		"Is `indexSearchable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	missingFilterableMetaCountFormat = "Searching by property '%s' count requires inverted index. " +
		"Is `indexFilterable` option of property '%s' enabled? " +
		"Set it to `true` or leave empty"
	noSearchablePropertiesFormat = "collection %q has no searchable properties for a keyword search. " +
		"Enable indexSearchable on a text property, or leave it empty so it defaults to on"
	// Every text property is disabled, so naming properties cannot succeed:
	// BM25F rejects each of them. The only fix is to enable indexSearchable.
	noSearchableWithDisabledFormat = "collection %q has no searchable properties for a keyword search: " +
		"indexSearchable is disabled on %s. Enable indexSearchable on a text property"
	skippedSearchableFormat = "indexSearchable is disabled on %s. " +
		"A BM25 or hybrid keyword search that does not name properties would skip %s and can return no keyword matches. " +
		"Enable indexSearchable, or set the search properties to the properties that should be searched"
)
