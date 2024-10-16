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
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rwhasher"
)

var _ ChecksumLogger = (*NoopChecksumLogger)(nil)

type NoopChecksumLogger struct{}

func NewNoopChecksumLogger() *NoopChecksumLogger {
	return &NoopChecksumLogger{}
}

func (c *NoopChecksumLogger) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (c *NoopChecksumLogger) Checksum(b []byte) (n int, err error) {
	return 0, nil
}

func (c *NoopChecksumLogger) Reset() {
}

func (c *NoopChecksumLogger) Flush() error {
	return nil
}

func (c *NoopChecksumLogger) Close() error {
	return nil
}

var _ rwhasher.ReaderHasher = (*NoopReaderHasher)(nil)

type NoopReaderHasher struct {
	r io.Reader
}

func NewNoopReaderHasher(r io.Reader) *NoopReaderHasher {
	return &NoopReaderHasher{r: r}
}

func (nr *NoopReaderHasher) Read(b []byte) (int, error) {
	return nr.r.Read(b)
}

func (nr *NoopReaderHasher) N() int {
	return 0
}

func (nr *NoopReaderHasher) Hash() []byte {
	return nil
}

func (nr *NoopReaderHasher) Reset() {
}

var _ ChecksumReader = (*NoopChecksumReader)(nil)

type NoopChecksumReader struct{}

func NewNoopChecksumReader() *NoopChecksumReader {
	return &NoopChecksumReader{}
}

func (r *NoopChecksumReader) HashSize() int {
	return 0
}

func (r *NoopChecksumReader) NextHash() ([]byte, error) {
	return nil, nil
}
