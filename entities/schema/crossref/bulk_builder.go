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
// The BulkBuilder makes some estimations for how much memory will be necesary
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
) ([]byte, error) {
	requiredSpace := len(bb.prefix) + len(className) + 1 + len(id)
	if bb.offset+requiredSpace >= len(bb.buf) {
		return nil, fmt.Errorf("fallback not implemented yet, %d vs %d", bb.offset+requiredSpace, len(bb.buf))
	}

	// copy the start pos, we will need this at the end to know what to return to
	// the caller
	start := bb.offset

	copy(bb.buf[bb.offset:], bb.prefix)
	bb.offset += len(bb.prefix)

	// This is a safe way, in case a class-name ever contains non-ASCII
	// characters. If we could be 100% sure that a class is ASCII-only, we could
	// remove this allocation and instead use the same copy-by-rune approach that
	// we use later on for the ID.
	classNameBuf := []byte(className)
	copy(bb.buf[bb.offset:], classNameBuf)
	bb.offset += len(classNameBuf)

	bb.buf[bb.offset] = '/' // The separating slash between class and ID
	bb.offset += 1

	for _, runeValue := range id {
		// We know that the UUID-string never contains non-ASCII characters. This
		// means it safe to convert the uint32-rune into a uint8. This allows us to
		// copy char by char without any additional allocs
		bb.buf[bb.offset] = uint8(runeValue)
		bb.offset += 1
	}

	return bb.buf[start:bb.offset], nil
}

func (bb *BulkBuilder) LegacyIDOnly(id strfmt.UUID) ([]byte, error) {
	requiredSpace := len(bb.prefix) + len(id)
	if bb.offset+requiredSpace >= len(bb.buf) {
		return nil, fmt.Errorf("fallback not implemented yet")
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

	return bb.buf[start:bb.offset], nil
}
