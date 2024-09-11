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

package lsmkv

import (
	"errors"
	"fmt"
	"time"
)

var (
	NotFound = errors.New("not found")
	Deleted  = errors.New("deleted")
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
