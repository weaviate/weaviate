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
	"hash/crc32"
)

// Fixed table for crc32.IEEE
var ieeeTable = crc32.MakeTable(crc32.IEEE)

// CRC32Resumable implements hash.Hash32 and supports reseeding.
// Always uses crc32.IEEE polynomial.
type CRC32Resumable struct {
	crc uint32
}

// NewCRC32Resumable creates a new CRC32Resumable starting at a given CRC seed.
func NewCRC32Resumable(seed uint32) *CRC32Resumable {
	return &CRC32Resumable{crc: seed}
}

// Write updates the CRC with data.
func (c *CRC32Resumable) Write(p []byte) (n int, err error) {
	c.crc = crc32.Update(c.crc, ieeeTable, p)
	return len(p), nil
}

// Sum returns the checksum appended to b in big-endian order.
func (c *CRC32Resumable) Sum(b []byte) []byte {
	s := c.Sum32()
	return append(b,
		byte(s>>24),
		byte(s>>16),
		byte(s>>8),
		byte(s),
	)
}

// Reset resets the CRC to zero.
func (c *CRC32Resumable) Reset() {
	c.crc = 0
}

// Size returns the number of bytes Sum will return (4).
func (c *CRC32Resumable) Size() int { return 4 }

// BlockSize returns the block size of the hash (1).
func (c *CRC32Resumable) BlockSize() int { return 1 }

// Sum32 returns the current CRC value.
func (c *CRC32Resumable) Sum32() uint32 { return c.crc }

// Reseed manually sets a new CRC seed value.
func (c *CRC32Resumable) Reseed(seed uint32) {
	c.crc = seed
}
