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

package lsmkv

import (
	"fmt"
	"io"
	"os"
)

// batchPread falls back to sequential ReadAt on non-Linux platforms.
func batchPread(positions []secondaryNodePos, results [][]byte) error {
	return batchPreadSequential(positions, results)
}

// batchPreadSequential reads each pread-backed position sequentially using
// os.File.ReadAt, which is available on all platforms.
func batchPreadSequential(positions []secondaryNodePos, results [][]byte) error {
	for i, p := range positions {
		if p.inMemoryData != nil || p.deleted || (p.fd == 0 && p.length == 0) {
			continue
		}
		buf := make([]byte, p.length)
		f := os.NewFile(uintptr(p.fd), "")
		if f == nil {
			return fmt.Errorf("invalid fd %d", p.fd)
		}
		n, err := f.ReadAt(buf, p.offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("ReadAt fd=%d offset=%d: %w", p.fd, p.offset, err)
		}
		if n != int(p.length) {
			return fmt.Errorf("short read fd=%d: got %d, want %d", p.fd, n, p.length)
		}
		results[i] = buf
	}
	return nil
}
