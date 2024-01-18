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

package storagestate

import "errors"

const (
	StatusReadOnly Status = "READONLY"
	StatusIndexing Status = "INDEXING"
	StatusReady    Status = "READY"
)

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
	default:
		err = ErrInvalidStatus
	}

	return
}
