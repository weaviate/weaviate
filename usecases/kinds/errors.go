/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import "fmt"

// ErrInvalidUserInput indicates a client-side error
type ErrInvalidUserInput error

func newErrInvalidUserInput(format string, args ...interface{}) ErrInvalidUserInput {
	return ErrInvalidUserInput(fmt.Errorf(format, args...))
}

// ErrInternal indicates something went wrong during processing
type ErrInternal error

func newErrInternal(format string, args ...interface{}) ErrInternal {
	return ErrInternal(fmt.Errorf(format, args...))
}

// ErrNotFound indicates the desired resource doesn't exist
type ErrNotFound error

func newErrNotFound(format string, args ...interface{}) ErrNotFound {
	return ErrNotFound(fmt.Errorf(format, args...))
}
