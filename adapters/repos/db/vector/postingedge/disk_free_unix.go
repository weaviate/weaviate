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
//go:build !windows

package postingedge

import "syscall"

// diskFree is how many bytes whoever runs the index can still write to the
// disk a directory is on.
func diskFree(dir string) (uint64, error) {
	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0, err
	}
	// Bavail is not a uint64 on every Unix.
	return uint64(fs.Bavail) * uint64(fs.Bsize), nil
}
