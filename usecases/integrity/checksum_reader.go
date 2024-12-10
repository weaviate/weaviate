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

package integrity

import (
	"hash"
	"hash/crc32"
	"io"
)

type ChecksumReader interface {
	io.Reader
	N() int
	Hash() []byte
	Reset()
}

var _ ChecksumReader = (*CRC32Reader)(nil)

type CRC32Reader struct {
	r    io.Reader
	n    int
	hash hash.Hash32
}

func NewCRC32Reader(r io.Reader) *CRC32Reader {
	return &CRC32Reader{
		r:    r,
		hash: crc32.NewIEEE(),
	}
}

func (rc *CRC32Reader) Read(p []byte) (n int, err error) {
	n, err = rc.r.Read(p)
	rc.n += n
	rc.hash.Write(p[:n])
	return n, err
}

func (rc *CRC32Reader) N() int {
	return rc.n
}

func (rc *CRC32Reader) Hash() []byte {
	return rc.hash.Sum(nil)
}

func (rc *CRC32Reader) Reset() {
	rc.n = 0
	rc.hash.Reset()
}
