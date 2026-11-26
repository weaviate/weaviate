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

package file

import (
	"os"
	"syscall"
)

// NameMax returns the longest file name dir's filesystem takes, in bytes.
func NameMax(dir string) (int, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, &os.PathError{Op: "statfs", Path: dir, Err: err}
	}
	return int(st.Namelen), nil
}
