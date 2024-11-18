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

package commitlog

import (
	"bufio"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"os"

	"github.com/weaviate/weaviate/entities/errorcompounder"
)

type ChecksumLogger interface {
	Write(b []byte) (n int, err error)
	Checksum(b []byte) (n int, err error)
	Reset()
	Flush() error
	Close() error
}

var _ ChecksumLogger = (*CRC32ChecksumLogger)(nil)

type CRC32ChecksumLogger struct {
	f    *os.File
	wbuf *bufio.Writer
	hash hash.Hash32
}

func OpenCRC32ChecksumLogger(name string, flag int, perm fs.FileMode) (*CRC32ChecksumLogger, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &CRC32ChecksumLogger{
		f:    f,
		wbuf: bufio.NewWriter(f),
		hash: crc32.NewIEEE(),
	}, nil
}

func (c *CRC32ChecksumLogger) Write(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	return c.hash.Write(b)
}

func (c *CRC32ChecksumLogger) Checksum(b []byte) (n int, err error) {
	n, err = c.Write(b)
	if err != nil {
		return n, err
	}

	return c.wbuf.Write(c.hash.Sum(nil))
}

func (c *CRC32ChecksumLogger) Reset() {
	c.hash.Reset()
}

func (c *CRC32ChecksumLogger) Flush() error {
	return c.wbuf.Flush()
}

func (c *CRC32ChecksumLogger) Close() error {
	ec := &errorcompounder.ErrorCompounder{}
	ec.Add(c.Flush())
	ec.Add(c.f.Close())
	return ec.ToError()
}

var _ ChecksumReader = (*CRC32ChecksumReader)(nil)

type CRC32ChecksumReader struct {
	r io.Reader
}

func NewCRC32ChecksumReader(r io.Reader) *CRC32ChecksumReader {
	return &CRC32ChecksumReader{r: r}
}

func (cr *CRC32ChecksumReader) HashSize() int {
	return 4
}

func (cr *CRC32ChecksumReader) NextHash() ([]byte, error) {
	var b [4]byte
	_, err := io.ReadFull(cr.r, b[:])
	if err != nil {
		return nil, err
	}
	return b[:], nil
}
