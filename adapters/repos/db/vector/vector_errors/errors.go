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

package vector_errors

import (
	"errors"
)

var (
	ErrVectorLengthDoesNotMatch = errors.New("cannot vector-search with vectors of different length")
	ErrNoVectorSearch           = errors.New("cannot vector-search on a class not vector-indexed")
)
