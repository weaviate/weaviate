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

//go:build !linux

// Package iouring is a no-op stub for non-Linux platforms where io_uring is
// not available.
package iouring

import "errors"

var errNotAvailable = errors.New("io_uring is only available on Linux")

// ReadOp describes a single pread operation (unused on non-Linux).
type ReadOp struct {
	Fd       int
	Offset   int64
	Buf      []byte
	UserData uint64
}

// Result is the completion result for one ReadOp (unused on non-Linux).
type Result struct {
	UserData uint64
	Res      int32
}

// Ring is a no-op placeholder on non-Linux platforms.
type Ring struct{}

// NewRing always returns an error on non-Linux platforms.
func NewRing(_ uint32) (*Ring, error) { return nil, errNotAvailable }

// Close is a no-op.
func (r *Ring) Close() error { return nil }

// SubmitAndWaitAll always returns an error on non-Linux platforms.
func (r *Ring) SubmitAndWaitAll(_ []ReadOp) ([]Result, error) {
	return nil, errNotAvailable
}
