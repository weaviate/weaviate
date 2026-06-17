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

func IsTransient(err error) bool {
	if errors.Is(err, ErrNotEnoughMemory) {
		return true
	}

	if errors.Is(err, ErrNotEnoughMappings) {
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

	return false
}

var (
	ErrNotEnoughMemory   = fmt.Errorf("not enough memory")
	ErrNotEnoughMappings = fmt.Errorf("not enough memory mappings")
)

func NewNotEnoughMemory(msg string) error {
	return fmt.Errorf("%s: %w", msg, ErrNotEnoughMemory)
}
