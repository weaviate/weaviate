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

package inverted

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"
)

// Page Design
// | Bytes     | Description                                      |
// | --------- | ------------------------------------------------ |
// | start     | page is now 0
// | 0-1       | uint16 pointer to last index byte
// | 2-3       | uint16 pointer for property name length
// | 4-n       | property name
// | ...       | repeat length+pointer pattern
// | 3584-3840 | second property buckets (64 buckets of float32)
// | 3840-4096 | first property buckets
// | repeat    | page is now 1, repeat all of above
//
// Fixed Assumptions:
//   - First two bytes always used to indicate end of index, minimal value is 02,
//     as the first possible value with index length=0 is after the two bytes
//     themselves.
//   - 64 buckets of float32 per property (=256B per prop), excluding the index
//   - One index row is always 4+len(propName), consisting of a uint16 prop name
//     length pointer, the name itself and an offset pointer pointing to the start
//     (first byte) of the buckets
//
// The counter to the last index byte is only an uint16, so it can at maximum address 65535. This will overflow when the
// 16th page is added (eg at page=15). To avoid a crash an error is returned in this case, but we will need to change
// the byteformat to fix this.
type PropertyLengthTracker struct {
	file  *os.File
	path  string
	pages []byte
	sync.Mutex
}

func NewPropertyLengthTracker(path string) (*PropertyLengthTracker, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	t := &PropertyLengthTracker{
		pages: nil,
		file:  f,
		path:  path,
	}

	if stat.Size() > 0 {
		// the file has existed before, we need to initialize with its content, we
		// can read the entire contents into memory
		existingPages, err := io.ReadAll(f)
		if err != nil {
			return nil, errors.Wrap(err, "read initial count from file")
		}

		if len(existingPages)%4096 != 0 {
			return nil, errors.Errorf(
				"failed sanity check, prop len tracker file %s has length %d", path,
				len(existingPages))
		}

		t.pages = existingPages
	} else {
		// this is the first time this is being created, initialize with an empty
		// page
		t.pages = make([]byte, 4096)
		// set initial end-of-index offset to 2
		binary.LittleEndian.PutUint16(t.pages[0:2], 2)
	}

	return t, nil
}

func (t *PropertyLengthTracker) BucketCount(propName string, bucket uint16) (uint16, error) {
	t.Lock()
	defer t.Unlock()

	page, offset, ok := t.propExists(propName)
	if !ok {
		return 0, fmt.Errorf("property %v does not exist in OldPropertyLengthTracker", propName)
	}

	offset = offset + page*4096

	o := offset + (bucket * 4)
	v := binary.LittleEndian.Uint32(t.pages[o : o+4])
	count := math.Float32frombits(v)

	return uint16(count), nil
}

func (t *PropertyLengthTracker) PropertyNames() []string {
	var names []string
	pages := len(t.pages) / int(4096)
	for page := 0; page < pages; page++ {
		pageStart := page * int(4096)

		relativeEOI := binary.LittleEndian.Uint16(t.pages[pageStart : pageStart+2]) // t.uint16At(pageStart)
		EOI := pageStart + int(relativeEOI)

		offset := int(pageStart) + 2
		for offset < EOI {
			propNameLength := int(binary.LittleEndian.Uint16(t.pages[offset : offset+2])) // int(t.uint16At(offset))
			offset += 2

			propName := t.pages[offset : offset+propNameLength]
			offset += propNameLength

			offset += 2

			names = append(names, string(propName))
		}
	}
	return names
}

func (t *PropertyLengthTracker) TrackProperty(propName string, value float32) error {
	t.Lock()
	defer t.Unlock()

	var page uint16
	var relBucketOffset uint16
	if p, o, ok := t.propExists(propName); ok {
		page = p
		relBucketOffset = o
	} else {
		var err error
		page, relBucketOffset, err = t.addProperty(propName)
		if err != nil {
			return err
		}
	}

	bucketOffset := page*4096 + relBucketOffset + t.bucketFromValue(value)*4

	v := binary.LittleEndian.Uint32(t.pages[bucketOffset : bucketOffset+4])
	currentValue := math.Float32frombits(v)
	currentValue += 1
	v = math.Float32bits(currentValue)
	binary.LittleEndian.PutUint32(t.pages[bucketOffset:bucketOffset+4], v)
	return nil
}

func (t *PropertyLengthTracker) UnTrackProperty(propName string, value float32) error {
	t.Lock()
	defer t.Unlock()

	var page uint16
	var relBucketOffset uint16
	if p, o, ok := t.propExists(propName); ok {
		page = p
		relBucketOffset = o
	} else {
		return fmt.Errorf("property %v does not exist in OldPropertyLengthTracker", propName)
	}

	bucketOffset := page*4096 + relBucketOffset + t.bucketFromValue(value)*4

	v := binary.LittleEndian.Uint32(t.pages[bucketOffset : bucketOffset+4])
	currentValue := math.Float32frombits(v)
	currentValue -= 1
	v = math.Float32bits(currentValue)
	binary.LittleEndian.PutUint32(t.pages[bucketOffset:bucketOffset+4], v)
	return nil
}

// propExists returns page number, relative offset on page, and a bool whether
// the prop existed at all. The first to values have no meaning if the latter
// is false
func (t *PropertyLengthTracker) propExists(needle string) (uint16, uint16, bool) {
	pages := len(t.pages) / 4096
	for page := 0; page < pages; page++ {
		pageStart := page * 4096

		relativeEOI := binary.LittleEndian.Uint16(t.pages[pageStart : pageStart+2])
		EOI := pageStart + int(relativeEOI)

		offset := int(pageStart) + 2
		for offset < EOI {
			propNameLength := int(binary.LittleEndian.Uint16(
				t.pages[offset : offset+2]))
			offset += 2

			propName := t.pages[offset : offset+propNameLength]
			offset += propNameLength
			bucketPointer := binary.LittleEndian.Uint16(
				t.pages[offset : offset+2])
			offset += 2

			if string(propName) == needle {
				return uint16(page), bucketPointer, true
			}

		}
	}
	return 0, 0, false
}

func (t *PropertyLengthTracker) addProperty(propName string) (uint16, uint16, error) {
	page := uint16(0)

	for {
		propNameBytes := []byte(propName)
		t.createPageIfNotExists(page)
		pageStart := page * 4096
		lastBucketOffset := pageStart + 4096

		relativeOffset := binary.LittleEndian.Uint16(t.pages[pageStart : pageStart+2])
		offset := pageStart + relativeOffset
		if relativeOffset != 2 {
			// relative offset is other than 2, so there are also props in. This
			// means we can take the value of offset-2 to read the bucket offset
			lastBucketOffset = pageStart + binary.LittleEndian.
				Uint16(t.pages[offset-2:offset])
		}

		if !t.canPageFit(propNameBytes, offset, lastBucketOffset) {
			page++
			// overflow of uint16 variable that tracks the size of the tracker
			if page > 15 {
				return 0, 0, fmt.Errorf("could not add property %v, to PropertyLengthTracker, because the total"+
					"length of all properties is too long", propName)
			}
			continue
		}

		propNameLength := uint16(len(propNameBytes))
		binary.LittleEndian.PutUint16(t.pages[offset:offset+2], propNameLength)
		offset += 2
		copy(t.pages[offset:offset+propNameLength], propNameBytes)
		offset += propNameLength

		newBucketOffset := lastBucketOffset - 256 - pageStart
		binary.LittleEndian.PutUint16(t.pages[offset:offset+2], newBucketOffset)
		offset += 2

		// update end of index offset for page, since the prop name index has
		// now grown
		binary.LittleEndian.PutUint16(t.pages[pageStart:pageStart+2], offset-pageStart)
		return page, newBucketOffset, nil
	}
}

func (t *PropertyLengthTracker) canPageFit(propName []byte,
	offset uint16, lastBucketOffset uint16,
) bool {
	// lastBucketOffset represents the end of the writable area, offset
	// represents the start, which means we can take the delta to see // how
	// much space is left on this page
	spaceLeft := lastBucketOffset - offset

	// we need to write 256 bytes for the buckets, plus two pointers of uint16
	spaceNeeded := uint16(len(propName)+4) + 256

	return spaceLeft >= spaceNeeded
}

func (t *PropertyLengthTracker) bucketFromValue(value float32) uint16 {
	if value <= 5.00 {
		return uint16(value) - 1
	}

	bucket := int(math.Log(float64(value)/4.0)/math.Log(1.25) + 4)
	if bucket > 63 {
		return 64
	}
	return uint16(bucket)
}

func (t *PropertyLengthTracker) valueFromBucket(bucket uint16) float32 {
	if bucket <= 5 {
		return float32(bucket + 1)
	}

	return float32(4 * math.Pow(1.25, float64(bucket)-3.5))
}

func (t *PropertyLengthTracker) PropertyMean(propName string) (float32, error) {
	t.Lock()
	defer t.Unlock()

	page, offset, ok := t.propExists(propName)
	if !ok {
		return 0, nil
	}

	sum := float32(0)
	totalCount := float32(0)
	bucket := uint16(0)

	offset = offset + page*4096
	for o := offset; o < offset+256; o += 4 {
		v := binary.LittleEndian.Uint32(t.pages[o : o+4])
		count := math.Float32frombits(v)
		sum += float32(t.valueFromBucket(bucket)) * count
		totalCount += count

		bucket++
	}

	if totalCount == 0 {
		return 0, nil
	}

	return sum / totalCount, nil
}

func (t *PropertyLengthTracker) PropertyTally(propName string) (int, int, float32, error) {
	t.Lock()
	defer t.Unlock()

	page, offset, ok := t.propExists(propName)
	if !ok {
		return 0, 0, 0, nil
	}

	sum := float32(0)
	totalCount := float32(0)
	bucket := uint16(0)

	offset = offset + page*4096
	for o := offset; o < offset+256; o += 4 {
		v := binary.LittleEndian.Uint32(t.pages[o : o+4])
		count := math.Float32frombits(v)
		sum += float32(t.valueFromBucket(bucket)) * count
		totalCount += count

		bucket++
	}

	if totalCount == 0 {
		return 0, 0, 0, nil
	}

	return int(sum), int(totalCount), sum / totalCount, nil
}

func (t *PropertyLengthTracker) createPageIfNotExists(page uint16) {
	if uint16(len(t.pages))/4096-1 < page {
		// we need to grow the page buffer
		newPages := make([]byte, uint64(page)*4096+4096)
		copy(newPages[:len(t.pages)], t.pages)

		// the new page must have the correct offset initialized
		binary.LittleEndian.PutUint16(newPages[page*4096:page*4096+2], 2)
		t.pages = newPages
	}
}

func (t *PropertyLengthTracker) Flush() error {
	t.Lock()
	defer t.Unlock()

	if err := t.file.Truncate(int64(len(t.pages))); err != nil {
		return errors.Wrap(err, "truncate prop tracker file to correct length")
	}

	if _, err := t.file.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek to beginning of prop tracker file")
	}

	if _, err := t.file.Write(t.pages); err != nil {
		return errors.Wrap(err, "flush page content to disk")
	}

	return nil
}

func (t *PropertyLengthTracker) Close() error {
	if err := t.Flush(); err != nil {
		return errors.Wrap(err, "flush before closing")
	}

	t.Lock()
	defer t.Unlock()

	if err := t.file.Close(); err != nil {
		return errors.Wrap(err, "close prop length tracker file")
	}

	t.pages = nil

	return nil
}

func (t *PropertyLengthTracker) Drop() error {
	t.Lock()
	defer t.Unlock()

	if err := t.file.Close(); err != nil {
		_ = err
		// explicitly ignore error
	}

	t.pages = nil

	if err := os.Remove(t.path); err != nil {
		return errors.Wrap(err, "remove prop length tracker state from disk")
	}

	return nil
}

func (t *PropertyLengthTracker) FileName() string {
	return t.file.Name()
}
