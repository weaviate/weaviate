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
//go:build windows

package postingedge

import "golang.org/x/sys/windows"

// diskFree is how many bytes whoever runs the index can still write to the
// disk a directory is on.
func diskFree(dir string) (uint64, error) {
	var available, total, free uint64
	if err := windows.GetDiskFreeSpaceEx(windows.StringToUTF16Ptr(dir), &available, &total, &free); err != nil {
		return 0, err
	}
	return available, nil
}
