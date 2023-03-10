//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package crossref

import (
	"fmt"

	"github.com/go-openapi/strfmt"
)

// BulkBuilder is a low-alloc tool to build many beacon strings (as []byte). It
// is optimized to allocate just once as opposed to once per ID. This makes it
// considerably faster when generating 100s of thousand of beacons strings. The
// main intended use case for this is building propValuePairs in ref-filters.
//
// The BulkBuilder makes some estimations for how much memory will be necessary
// based on expected input params. If those requirements get exceeded, it will
// still be safe to use, but will fallback to allocating dynamically.
type BulkBuilder struct {
	buf    []byte
	prefix []byte
	offset int
}

func NewBulkBuilderWithEstimates(expectedCount int, exampleClassName string,
	overheadRatio float64,
) *BulkBuilder {
	prefix := []byte("weaviate://localhost/")

	lenOfTypicalClassName := int(float64(len(exampleClassName)) * overheadRatio)
	predictedSize := expectedCount * (len(prefix) + 1 + lenOfTypicalClassName + 36)

	bb := &BulkBuilder{
		buf:    make([]byte, predictedSize),
		prefix: prefix,
	}

	return bb
}

func (bb *BulkBuilder) ClassAndID(className string,
	id strfmt.UUID,
) []byte {
	requiredSpace := len(bb.prefix) + len(className) + 1 + len(id)
	if bb.offset+requiredSpace > len(bb.buf) {
		return bb.fallbackWithClassName(className, id)
	}

	// copy the start pos, we will need this at the end to know what to return to
	// the caller
	start := bb.offset

	bb.offset += copy(bb.buf[bb.offset:], bb.prefix)

	// This is a safe way, in case a class-name ever contains non-ASCII
	// characters. If we could be 100% sure that a class is ASCII-only, we could
	// remove this allocation and instead use the same copy-by-rune approach that
	// we use later on for the ID.
	classNameBuf := []byte(className)
	bb.offset += copy(bb.buf[bb.offset:], classNameBuf)

	// The separating slash between class and ID
	bb.buf[bb.offset] = '/'
	bb.offset += 1

	for _, runeValue := range id {
		// We know that the UUID-string never contains non-ASCII characters. This
		// means it safe to convert the uint32-rune into a uint8. This allows us to
		// copy char by char without any additional allocs
		bb.buf[bb.offset] = uint8(runeValue)
		bb.offset += 1
	}

	return bb.buf[start:bb.offset]
}

func (bb *BulkBuilder) LegacyIDOnly(id strfmt.UUID) []byte {
	requiredSpace := len(bb.prefix) + len(id)
	if bb.offset+requiredSpace >= len(bb.buf) {
		return bb.fallbackWithoutClassName(id)
	}

	// copy the start pos, we will need this at the end to know what to return to
	// the caller
	start := bb.offset

	copy(bb.buf[bb.offset:], bb.prefix)
	bb.offset += len(bb.prefix)

	for _, runeValue := range id {
		// We know that the UUID-string never contains non-ASCII characters. This
		// means it safe to convert the uint32-rune into a uint8. This allows us to
		// copy char by char without any additional allocs
		bb.buf[bb.offset] = uint8(runeValue)
		bb.offset += 1
	}

	return bb.buf[start:bb.offset]
}

func (bb *BulkBuilder) fallbackWithClassName(
	className string, id strfmt.UUID,
) []byte {
	return []byte(fmt.Sprintf("%s%s/%s", bb.prefix, className, id))
}

func (bb *BulkBuilder) fallbackWithoutClassName(id strfmt.UUID) []byte {
	return []byte(fmt.Sprintf("%s%s", bb.prefix, id))
}
