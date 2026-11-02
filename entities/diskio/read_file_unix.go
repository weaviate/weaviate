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

//go:build unix

package diskio

import (
	"errors"
	"os"
	"syscall"
)

// ReadFileExact reads the regular file at path into a slice sized by fstat, so
// it suits only files nobody writes during the read. It calls the syscalls
// directly to skip the netpoller setup os.Open does for every file.
func ReadFileExact(path string) ([]byte, error) {
	var fd int
	var err error
	for {
		fd, err = syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC, 0)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	defer syscall.Close(fd)

	var st syscall.Stat_t
	for {
		err = syscall.Fstat(fd, &st)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		return nil, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return nil, &os.PathError{Op: "read", Path: path, Err: errNotRegularFile}
	}

	data := make([]byte, st.Size)
	n := 0
	for n < len(data) {
		m, err := syscall.Read(fd, data[n:])
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return nil, &os.PathError{Op: "read", Path: path, Err: err}
		}
		if m == 0 {
			break
		}
		n += m
	}
	return data[:n], nil
}
