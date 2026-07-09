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

//go:build linux

package lsmkv

import (
	"os"

	"golang.org/x/sys/unix"
)

// fadviseSequential hints the kernel that the file will be read
// sequentially. This doubles the default read-ahead window and may cause
// the kernel to prioritize evicting already-read pages (kernel version
// dependent). Used when a cursor scans from start to end.
func fadviseSequential(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
}

// fadviseRandom hints the kernel that the file will be read at random
// offsets, disabling read-ahead on the descriptor. Without it, every random
// point read drags in a full read-ahead window (128KB by default) of useless
// neighbouring pages; on point-lookup-heavy buckets this saturates a cloud
// volume's throughput cap long before its IOPS limit.
func fadviseRandom(f *os.File) error {
	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM)
}

// madviseRandom is the equivalent hint for a mapped region: it disables
// read-ahead and fault-around for page faults on the mapping. Needed in
// addition to fadviseRandom because large segments are mmapped and index
// lookups fault the mapping rather than reading through the descriptor.
func madviseRandom(b []byte) error {
	return unix.Madvise(b, unix.MADV_RANDOM)
}

// fadviseDontNeed tells the kernel that the file's pages are no longer
// needed and can be evicted from the page cache. This prevents a completed
// sequential scan from polluting the cache with pages that won't be
// accessed again, freeing space for other workloads. When the underlying
// inode is shared (e.g. via hard links), only pages not actively mapped by
// other file descriptors are evicted.
func fadviseDontNeed(f *os.File, size int64) error {
	return unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_DONTNEED)
}
