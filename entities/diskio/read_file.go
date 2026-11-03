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

package diskio

import (
	"errors"
	"io"
	"os"
)

// errNotRegularFile rejects a pipe, device or directory, whose size says
// nothing about how many bytes a read returns.
var errNotRegularFile = errors.New("not a regular file")

// readFileExactPortable is ReadFileExact built on package os. It is untagged so
// the unix tests run the same table over it.
func readFileExactPortable(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, &os.PathError{Op: "read", Path: path, Err: errNotRegularFile}
	}
	data := make([]byte, info.Size())
	n, err := io.ReadFull(f, data)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return data[:n], nil
}
