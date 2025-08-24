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

type ChecksumWriter interface {
	io.Writer
	N() int
	Hash() []byte
	HashWrite([]byte) (int, error)
	Reset()
}

var _ ChecksumWriter = (*CRC32Writer)(nil)

type CRC32Writer struct {
	w    io.Writer
	n    int
	hash hash.Hash32
}

func NewCRC32Writer(w io.Writer) *CRC32Writer {
	return &CRC32Writer{
		w:    w,
		hash: crc32.NewIEEE(),
	}
}

func NewCRC32WriterWithSeed(w io.Writer, seed uint32) *CRC32Writer {
	return &CRC32Writer{
		w:    w,
		hash: NewCRC32Resumable(seed),
	}
}

func (wc *CRC32Writer) Write(p []byte) (n int, err error) {
	n, err = wc.w.Write(p)
	wc.n += n
	wc.hash.Write(p[:n])
	return n, err
}

func (wc *CRC32Writer) HashWrite(p []byte) (int, error) {
	return wc.hash.Write(p)
}

func (wc *CRC32Writer) N() int {
	return wc.n
}

func (wc *CRC32Writer) Hash() []byte {
	return wc.hash.Sum(nil)
}

func (wc *CRC32Writer) Reset() {
	wc.n = 0
	wc.hash.Reset()
}
