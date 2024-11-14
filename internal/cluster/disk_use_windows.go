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

package cluster

import (
	"golang.org/x/sys/windows"
)

// diskSpace return the disk space usage
func diskSpace(path string) (DiskUsage, error) {
	var freeBytesAvailableToCaller, totalBytes, totalFreeBytes uint64

	err := windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(path),
		&freeBytesAvailableToCaller,
		&totalBytes,
		&totalFreeBytes,
	)
	if err != nil {
		return DiskUsage{}, err
	}

	return DiskUsage{
		Total:     totalBytes,
		Available: freeBytesAvailableToCaller,
	}, nil
}
