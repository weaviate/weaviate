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

package reader

import (
	"fmt"
	"os"
)

type ContentReader interface {
	ReadAt(p []byte, off int64) (n int, err error)
	Size() (int64, error)
	Close() error
}

var _ ContentReader = (*LocalContentReader)(nil)

type S3ContentReader struct {
	// TODO: bucket config
}

type LocalContentReader struct {
	f    *os.File
	size int64
}

func NewLocalContentReader(path string) (*LocalContentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &LocalContentReader{f: f, size: fi.Size()}, nil
}

func (r *LocalContentReader) ReadAt(p []byte, off int64) (n int, err error) {
	return r.f.ReadAt(p, off)
}

func (r *LocalContentReader) Size() (int64, error) {
	return r.size, nil
}

func (r *LocalContentReader) Close() error {
	return r.f.Close()
}
