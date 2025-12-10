//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"errors"
)

var (
	// MsgCLevel consistency level cannot be achieved
	MsgCLevel = "cannot achieve consistency level"

	ErrReplicas = errors.New("cannot reach enough replicas")
	ErrRepair   = errors.New("read repair error")
	ErrRead     = errors.New("read error")

	ErrNoDiffFound = errors.New("no diff found")
)

// // ReplicasError is a custom error type for replica-related errors
// // that always prefixes wrapped errors with "cannot reach enough replicas: "
// type ReplicasError struct {
// 	err error
// }

// // Error implements the error interface
// func (e *ReplicasError) Error() string {
// 	if e.err != nil {
// 		return fmt.Sprintf("cannot reach enough replicas: %v", e.err)
// 	}
// 	return "cannot reach enough replicas"
// }

// // Unwrap returns the underlying error for error wrapping support
// func (e *ReplicasError) Unwrap() error {
// 	return e.err
// }

// // NewReplicasError creates a new ReplicasError that wraps the given error
// // The error message will always be prefixed with "cannot reach enough replicas: "
// func NewReplicasError(err error) *ReplicasError {
// 	return &ReplicasError{err: err}
// }

// // Is implements errors.Is for backward compatibility
// func (e *ReplicasError) Is(target error) bool {
// 	// Check if target is also a ReplicasError type
// 	if target == errReplicas {
// 		return true
// 	}
// 	var targetReplicasErr *ReplicasError
// 	if errors.As(target, &targetReplicasErr) {
// 		return true
// 	}
// 	// Check underlying error
// 	if e.err != nil {
// 		return errors.Is(e.err, target)
// 	}
// 	return false
// }

// // IsReplicasError reports whether err is a ReplicasError.
// // This is the idiomatic way to check for replica errors without exporting errReplicas.
// func IsReplicasError(err error) bool {
// 	return errors.Is(err, errReplicas)
// }
