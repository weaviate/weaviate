package inverted

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Page Design
// | Bytes     | Description                                      |
// | --------- | ------------------------------------------------ |
// | 0-1       | uint16 pointer to last index byte
// | 2-3       | uint16 pointer for property name length
// | 4-n       | property name
// | ...       | repeat length+pointer pattern
// | 3584-3840 | second property buckets (64 buckets of float32)
// | 3840-4096 | first property buckets
type PropertyLengthTracker struct {
	path string
	// file *os.File
	pages          []byte
	bucketsPerProp int
}

func NewPropertyLengthTracker(path string) *PropertyLengthTracker {
	// os.Open(path)

	t := &PropertyLengthTracker{
		pages:          make([]byte, 4096),
		bucketsPerProp: 64, // using float32 -> 256B per property
	}

	// set initial end-of-index offset to 2
	binary.LittleEndian.PutUint16(t.pages[0:2], 2)
	return t
}

func (t *PropertyLengthTracker) TrackProperty(propName string,
	value float32) {
	var page uint16
	var bucketOffset uint16
	if p, o, ok := t.propExists(propName); ok {
		page = p
		bucketOffset = o
	} else {
		page, bucketOffset = t.addProperty(propName)
	}

	_ = page
	bucketOffset = bucketOffset + t.bucketFromValue(value)*4

	v := binary.LittleEndian.Uint32(t.pages[bucketOffset : bucketOffset+4])
	currentValue := math.Float32frombits(v)
	currentValue += 1
	v = math.Float32bits(currentValue)
	binary.LittleEndian.PutUint32(t.pages[bucketOffset:bucketOffset+4], v)
}

func (t *PropertyLengthTracker) propExists(needle string) (uint16, uint16, bool) {
	// TODO: support multiple pages
	endOfIndex := binary.LittleEndian.Uint16(t.pages[0:2])
	if endOfIndex == 2 {
		return 0, 0, false
	}

	offset := uint16(2)
	for offset < endOfIndex {
		propNameLength := binary.LittleEndian.Uint16(
			t.pages[offset : offset+2])
		offset += 2

		propName := t.pages[offset : offset+propNameLength]
		offset += propNameLength
		bucketPointer := binary.LittleEndian.Uint16(
			t.pages[offset : offset+2])
		offset += 2

		if string(propName) == needle {
			return 0, bucketPointer, true
		}

	}
	return 0, 0, false
}

func (t *PropertyLengthTracker) addProperty(propName string) (uint16, uint16) {
	page := uint16(0)
	propNameBytes := []byte(propName)
	if !t.canPageFit(propNameBytes) {
		panic("page can't fit")
	}

	lastBucketOffset := uint16(4096)
	offset := binary.LittleEndian.Uint16(t.pages[0:2])
	if offset != 2 {
		panic("don't know how to calculate last bucket offset yet")
	}

	propNameLength := uint16(len(propNameBytes))
	binary.LittleEndian.PutUint16(t.pages[offset:offset+2], propNameLength)
	offset += 2
	copy(t.pages[offset:offset+propNameLength], propNameBytes)
	offset += propNameLength

	newBucketOffset := lastBucketOffset - 256
	binary.LittleEndian.PutUint16(t.pages[offset:offset+2], newBucketOffset)
	offset += 2

	// update end of index offset for page, since the prop name index has
	// now grown
	binary.LittleEndian.PutUint16(t.pages[0:2], offset)
	return page, newBucketOffset
}

func (t *PropertyLengthTracker) canPageFit(propName []byte) bool {
	// TODO: actually check for page space
	return true
}

func (t *PropertyLengthTracker) bucketFromValue(value float32) uint16 {
	switch true {
	case value <= 1.00:
		return 0
	case value <= 2.00:
		return 1
	case value <= 3.00:
		return 2
	case value <= 4.00:
		return 3
	// TODO
	default:
		return 63
	}
}

func (t *PropertyLengthTracker) PropertyMean(propName string) (float32, error) {
	page, offset, ok := t.propExists(propName)
	if !ok {
		return 0, nil
	}

	fmt.Printf("start: %d, end: %d\n", offset, offset+256)

	_ = page
	sum := float32(0)
	totalCount := float32(0)
	bucket := 0
	for o := offset; o < offset+256; o += 4 {
		v := binary.LittleEndian.Uint32(t.pages[o : o+4])
		count := math.Float32frombits(v)
		sum += float32(bucket+1) * count
		totalCount += count

		bucket++
	}

	return sum / totalCount, nil
}
