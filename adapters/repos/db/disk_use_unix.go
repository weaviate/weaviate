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

package db

import (
	"syscall"
)

func (db *DB) getDiskUse(diskPath string) diskUse {
	fs := syscall.Statfs_t{}

	err := syscall.Statfs(diskPath, &fs)
	if err != nil {
		db.logger.WithField("action", "read_disk_use").
			WithField("path", diskPath).
			Errorf("failed to read disk usage: %s", err)
	}

	return diskUse{
		total: fs.Blocks * uint64(fs.Bsize),
		free:  fs.Bfree * uint64(fs.Bsize),
		avail: fs.Bfree * uint64(fs.Bsize),
	}
}
