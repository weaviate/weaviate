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

package storagestate

import (
	"errors"
	"fmt"
)

const (
	StatusReadOnly    Status = "READONLY"
	StatusIndexing    Status = "INDEXING"
	StatusLoading     Status = "LOADING"
	StatusLazyLoading Status = "LAZY_LOADING"
	StatusReady       Status = "READY"
	StatusShutdown    Status = "SHUTDOWN"
)

// ErrStatusReadOnlyWithReason builds the operator-facing read-only error. It
// %w-wraps ErrStatusReadOnly — errors.Is classification (the transient-error
// classifier, the raft apply path's park decision) depends on the sentinel
// being in the chain — while keeping the exact message text unchanged.
var ErrStatusReadOnlyWithReason = func(reason string) error {
	return fmt.Errorf("%w due to: %v", ErrStatusReadOnly, reason)
}

var (
	ErrStatusReadOnly = errors.New("store is read-only")
	ErrInvalidStatus  = errors.New("invalid storage status")
)

type Status string

func (s Status) String() string {
	return string(s)
}

func ValidateStatus(in string) (status Status, err error) {
	switch in {
	case string(StatusReadOnly):
		status = StatusReadOnly
	case string(StatusIndexing):
		status = StatusIndexing
	case string(StatusReady):
		status = StatusReady
	case string(StatusShutdown):
		status = StatusShutdown
	default:
		err = ErrInvalidStatus
	}

	return status, err
}
