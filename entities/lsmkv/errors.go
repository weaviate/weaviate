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

package lsmkv

import (
	"errors"
	"fmt"
	"time"
)

var (
	NotFound = errors.New("not found")
	Deleted  = errors.New("deleted")

	// ErrCorruptIndex marks a segment index that cannot be read, as opposed to one
	// that does not hold the key. A segment's key lookups report it; the bucket
	// cursors panic on it, as on any error they cannot fold into their state.
	ErrCorruptIndex = errors.New("corrupt segment index")
)

type ErrDeleted struct {
	deletionTime time.Time
}

func NewErrDeleted(deletionTime time.Time) ErrDeleted {
	return ErrDeleted{deletionTime: deletionTime}
}

func (e ErrDeleted) DeletionTime() time.Time {
	return e.deletionTime
}

func (e ErrDeleted) Error() string {
	return fmt.Sprintf("%v: deletion time %s", Deleted, e.deletionTime)
}

// Unwrap returns Deleted error so to satisfy checks like errors.Is(err, lsmkv.Deleted)
func (e ErrDeleted) Unwrap() error {
	return Deleted
}

// IsNotFound and its siblings settle the bare sentinels and nil by comparison
// before calling errors.Is: segments return the sentinels unwrapped, and a merge
// cursor checks every segment's state for every key it serves, so errors.Is on
// those values is a measurable share of a full scan.
func IsNotFound(err error) bool {
	if err == nil || err == Deleted { //nolint:errorlint // fast path, errors.Is follows
		return false
	}
	return err == NotFound || errors.Is(err, NotFound) //nolint:errorlint // fast path, errors.Is follows
}

func IsDeleted(err error) bool {
	if err == nil || err == NotFound { //nolint:errorlint // fast path, errors.Is follows
		return false
	}
	return err == Deleted || errors.Is(err, Deleted) //nolint:errorlint // fast path, errors.Is follows
}

func IsDeletedOrNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err == NotFound || err == Deleted || //nolint:errorlint // fast path, errors.Is follows
		errors.Is(err, NotFound) || errors.Is(err, Deleted)
}
