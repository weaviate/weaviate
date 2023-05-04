//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import "fmt"

type MissingIndexError struct {
	message string
}

func NewMissingFilterableIndexError(propName string) error {
	return MissingIndexError{message: fmt.Sprintf("Filtering by property '%s' requires inverted index. "+
		"Is `indexFilterable` option of property '%s' enabled? "+
		"Set it to `true` or leave empty", propName, propName)}
}

func NewMissingSearchableIndexError(propName string) error {
	return MissingIndexError{message: fmt.Sprintf("Searching by property '%s' requires inverted index. "+
		"Is `indexSearchable` option of property '%s' enabled? "+
		"Set it to `true` or leave empty", propName, propName)}
}

func NewMissingFilterableMetaCountIndexError(propName string) error {
	return MissingIndexError{message: fmt.Sprintf("Searching by property '%s' count requires inverted index. "+
		"Is `indexFilterable` option of property '%s' enabled? "+
		"Set it to `true` or leave empty", propName, propName)}
}

func (e MissingIndexError) Error() string {
	return e.message
}
