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

//go:build windows

package db

import (
	"syscall"

	"golang.org/x/sys/windows"
)

func (db *DB) getDiskUse(diskPath string) diskUse {
	var freeBytesAvailable, totalNumberOfBytes, totalNumberOfFreeBytes uint64

	_, _ = syscall.UTF16PtrFromString(diskPath)

	err := windows.GetDiskFreeSpaceEx(
		syscall.StringToUTF16Ptr(diskPath),
		&freeBytesAvailable,
		&totalNumberOfBytes,
		&totalNumberOfFreeBytes,
	)
	if err != nil {
		db.logger.WithField("action", "read_disk_use").
			WithField("path", diskPath).
			Errorf("failed to read disk usage: %s", err)
	}

	return diskUse{
		total: totalNumberOfBytes,
		free:  totalNumberOfFreeBytes,
		avail: freeBytesAvailable,
	}
}
