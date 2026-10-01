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

package errors

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/weaviate/weaviate/entities/storagestate"
)

// IsMemoryPressure reports whether err is a deliberate memory load-shed rather than a fault
func IsMemoryPressure(err error) bool {
	return errors.Is(err, ErrNotEnoughMemory) || errors.Is(err, ErrNotEnoughMappings)
}

func IsTransient(err error) bool {
	if IsMemoryPressure(err) {
		return true
	}

	if errors.Is(err, storagestate.ErrStatusReadOnly) {
		return true
	}

	// Disk-full errors are transient: an operator (or automated job) can free
	// up or grow the disk, so retrying gives the recovery a chance to land
	// before we discard the batch.
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}

	// An HNSW insert that finds no node to start its neighbor search from is
	// retried: the state is created by concurrent inserts and tombstone cleanup
	// holding the few live nodes of a small graph under maintenance, and it
	// clears within milliseconds. Discarding the batch would silently leave the
	// vectors unindexed.
	if errors.Is(err, ErrNoUsableEntrypoint) {
		return true
	}

	return false
}

var (
	ErrNotEnoughMemory    = fmt.Errorf("not enough memory")
	ErrNotEnoughMappings  = fmt.Errorf("not enough memory mappings")
	ErrNoUsableEntrypoint = fmt.Errorf("no usable entrypoint")
)

func NewNotEnoughMemory(msg string) error {
	return fmt.Errorf("%s: %w", msg, ErrNotEnoughMemory)
}
