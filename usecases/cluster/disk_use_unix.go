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

//go:build !windows

package cluster

import (
	"syscall"
)

// diskSpace return the disk space usage
func diskSpace(path string) (DiskUsage, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return DiskUsage{}, err
	}
	return DiskUsage{
		Total:     fs.Blocks * uint64(fs.Bsize),
		Available: fs.Bavail * uint64(fs.Bsize),
	}, nil
}
