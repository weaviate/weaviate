//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package types

import "errors"

var (
	ErrInvalidRequest               = errors.New("invalid request")
	ErrCancellationImpossible       = errors.New("cancellation impossible")
	ErrDeletionImpossible           = errors.New("deletion impossible")
	ErrReplicationOperationNotFound = errors.New("replication operation not found")
	// ErrNotFound is a custom error that is used to indicate that a resource was not found.
	// We use it to return a specific error code from the RPC layer to ensure we don't retry an operation
	// returning an error indicating that the resource was not found.
	// We add E00001 to the error string to ensure it's unique and can be checked for specifically.
	// Otherwise it could be matched against any "not found" error.
	ErrNotFound = errors.New("E00001: not found")
)
