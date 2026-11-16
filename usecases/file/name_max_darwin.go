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

//go:build darwin

package file

import (
	"os"

	"golang.org/x/sys/unix"
)

// pcNameMax is _PC_NAME_MAX in <sys/unistd.h>, which golang.org/x/sys/unix
// does not export for darwin.
const pcNameMax = 4

// NameMax returns the longest file name dir's filesystem takes, in bytes.
func NameMax(dir string) (int, error) {
	n, err := unix.Pathconf(dir, pcNameMax)
	if err != nil {
		return 0, &os.PathError{Op: "pathconf", Path: dir, Err: err}
	}
	return n, nil
}
