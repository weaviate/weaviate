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
	"encoding/json"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var MAX_BUCKETS = 64

type ShardMetaData struct {
	BucketedData map[string]map[int]int
	SumData      map[string]int
	CountData    map[string]int
	ObjectCount  int
}

type JsonShardMetaData struct {
	path string
	data *ShardMetaData // Only this part is saved in the file
	sync.Mutex
	UnlimitedBuckets bool
	logger           logrus.FieldLogger
}

// This class replaces the old PropertyLengthTracker.  It fixes a bug and provides a
// simpler, easier to maintain implementation.  The format is future-proofed, new
// data can be added to the file without breaking old versions of Weaviate.
//
// * We need to know the mean length of all properties for BM25 calculations
// * The prop length tracker is an approximate tracker that uses buckets and simply counts the entries in the buckets
// * There is a precise global counter for the sum of all lengths and a precise global counter for the number of entries
// * It only exists for string/text (and their array forms) because these are the only prop types that can be used with BM25
// * It should probably always exist when indexSearchable is set on a text prop going forward
//
// Property lengths are put into one of 64 buckets.  The value of a bucket is given by the formula:
//
// float32(4 * math.Pow(1.25, float64(bucket)-3.5))
//
// Which as implemented gives bucket values of 0,1,2,3,4,5,6,8,10,13,17,21,26,33,41,52,65,81,101,127,158,198,248,310,387,484,606,757,947,1183,1479,1849,2312,2890,3612,4515,5644,7055,8819,11024,13780,17226,21532,26915,33644,42055,52569,65712,82140,102675,128344,160430,200537,250671,313339,391674,489593,611991,764989,956237,1195296,1494120,1867651,2334564
//
// These buckets are then recorded to disk.  The original implementation was a binary format where all the data was tracked using manual pointer arithmetic.  The new version tracks the statistics in a go map, and marshals that into JSON before writing it to disk.  There is no measurable difference in speed between these two implementations while importing data, however it appears to slow the queries by about 15% (while improving recall by ~25%).
//
// The new tracker is exactly compatible with the old format to enable migration, which is why there is a -1 bucket.  Altering the number of buckets or their values will break compatibility.
//
// Set UnlimitedBuckets to true for precise length tracking
//
// Note that some of the code in this file is forced by the need to be backwards-compatible with the old format.  Once we are confident that all users have migrated to the new format, we can remove the old format code and simplify this file.

// NewJsonShardMetaData creates a new tracker and loads the data from the given path.  If the file is in the old format, it will be converted to the new format.
func NewJsonShardMetaData(path string, logger logrus.FieldLogger) (t *JsonShardMetaData, err error) {
	// Recover and return empty tracker on panic
	defer func() {
		if r := recover(); r != nil {
			t.logger.Printf("Recovered from panic in NewJsonShardMetaData, original error: %v", r)
			t = &JsonShardMetaData{
				data:             &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0},
				path:             path,
				UnlimitedBuckets: false,
			}
			err = errors.Errorf("Recovered from panic in NewJsonShardMetaData, original error: %v", r)
		}
	}()

	t = &JsonShardMetaData{
		data:             &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0},
		path:             path,
		UnlimitedBuckets: false,
		logger:           logger,
	}

	// read the file into memory
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) { // File doesn't exist, probably a new class(or a recount), return empty tracker
			logger.Printf("WARNING: prop len tracker file %s does not exist, creating new tracker", path)
			t.Flush(false)
			return t, nil
		}
		return nil, errors.Wrap(err, "read property length tracker file:"+path)
	}

	if len(bytes) == 0 {
		return nil, errors.Errorf("failed sanity check, empty prop len tracker file %s has length 0.  Delete file and set environment variable RECOUNT_PROPERTIES_AT_STARTUP to true", path)
	}

	// We don't have data file versioning, so we try to parse it as json.  If the parse fails, it is probably the old format file, so we call the old format loader and copy everything across.
	if err = json.Unmarshal(bytes, &t.data); err != nil {
		// It's probably the old format file, load the old format and convert it to the new format
		plt, err := NewPropertyLengthTracker(path)
		if err != nil {
			return nil, errors.Wrap(err, "convert old property length tracker")
		}

		propertyNames := plt.PropertyNames()
		data := &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0}
		// Loop over every page and bucket in the old tracker and add it to the new tracker
		for _, name := range propertyNames {
			data.BucketedData[name] = make(map[int]int, MAX_BUCKETS)
			data.CountData[name] = 0
			data.SumData[name] = 0
			for i := 0; i <= MAX_BUCKETS; i++ {
				fromBucket := i
				if i == MAX_BUCKETS {
					fromBucket = -1
				}
				count, err := plt.BucketCount(name, uint16(fromBucket))
				if err != nil {
					return nil, errors.Wrap(err, "convert old property length tracker")
				}
				data.BucketedData[name][fromBucket] = int(count)
				value := float32(0)
				if fromBucket == -1 {
					value = 0
				} else {
					value = plt.valueFromBucket(uint16(fromBucket))
				}

				data.SumData[name] = data.SumData[name] + int(value)*int(count)
				data.CountData[name] = data.CountData[name] + int(count)
			}
		}
		t.data = data
		t.Flush(true)
		plt.Close()
		plt.Drop()
		t.Flush(false)
	}
	t.path = path

	// Make really sure we aren't going to crash on a nil pointer
	if t.data == nil {
		return nil, errors.Errorf("failed sanity check, prop len tracker file %s has nil data.  Delete file and set environment variable RECOUNT_PROPERTIES_AT_STARTUP to true", path)
	}
	return t, nil
}

func (t *JsonShardMetaData) Clear() {
	if t == nil {
		return
	}
	t.Lock()
	defer t.Unlock()

	t.data = &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0}
}

// Path to the file on disk
func (t *JsonShardMetaData) FileName() string {
	if t == nil {
		return ""
	}
	return t.path
}

func (t *JsonShardMetaData) TrackObjects(delta int) error {
	if t == nil {
		return nil
	}
	t.Lock()
	defer t.Unlock()

	t.data.ObjectCount = t.data.ObjectCount + delta
	return nil
}

// Adds a new value to the tracker
func (t *JsonShardMetaData) TrackProperty(propName string, value float32) error {
	if t == nil {
		return nil
	}
	t.Lock()
	defer t.Unlock()

	// Remove this check once we are confident that all users have migrated to the new format
	if t.data == nil {
		t.logger.Print("WARNING: t.data is nil in TrackProperty, initializing to empty tracker")
		t.data = &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0}
	}
	t.data.SumData[propName] = t.data.SumData[propName] + int(value)
	t.data.CountData[propName] = t.data.CountData[propName] + 1

	bucketId := t.bucketFromValue(value)
	if _, ok := t.data.BucketedData[propName]; ok {
		t.data.BucketedData[propName][int(bucketId)] = t.data.BucketedData[propName][int(bucketId)] + 1
	} else {

		t.data.BucketedData[propName] = make(map[int]int, 64+1)
		t.data.BucketedData[propName][int(bucketId)] = 1
	}

	return nil
}

// Removes a value from the tracker
func (t *JsonShardMetaData) UnTrackProperty(propName string, value float32) error {
	if t == nil {
		return nil
	}
	t.Lock()
	defer t.Unlock()

	// Remove this check once we are confident that all users have migrated to the new format
	if t.data == nil {
		t.logger.Print("WARNING: t.data is nil in TrackProperty, initializing to empty tracker")
		t.data = &ShardMetaData{make(map[string]map[int]int), make(map[string]int), make(map[string]int), 0}
	}
	t.data.SumData[propName] = t.data.SumData[propName] - int(value)
	t.data.CountData[propName] = t.data.CountData[propName] - 1

	bucketId := t.bucketFromValue(value)
	if _, ok := t.data.BucketedData[propName]; ok {
		t.data.BucketedData[propName][int(bucketId)] = t.data.BucketedData[propName][int(bucketId)] - 1
	} else {
		return errors.New("property not found")
	}

	return nil
}

// Returns the bucket that the given value belongs to
func (t *JsonShardMetaData) bucketFromValue(value float32) int {
	if t == nil {
		return 0
	}
	if t.UnlimitedBuckets {
		return int(value)
	}
	if value <= 5.00 {
		return int(value) - 1
	}

	bucket := int(math.Log(float64(value)/4.0)/math.Log(1.25) + 4)
	if bucket > MAX_BUCKETS-1 {
		return MAX_BUCKETS
	}
	return int(bucket)
}

// Returns the average length of the given property
func (t *JsonShardMetaData) PropertyMean(propName string) (float32, error) {
	if t == nil {
		return 0, nil
	}
	t.Lock()
	defer t.Unlock()

	sum, ok := t.data.SumData[propName]
	if !ok {
		return 0, nil
	}
	count, ok := t.data.CountData[propName]
	if !ok {
		return 0, nil
	}

	return float32(sum) / float32(count), nil
}

// returns totalPropertyLength, totalCount, average propertyLength = sum / totalCount, total propertylength, totalCount, error
func (t *JsonShardMetaData) PropertyTally(propName string) (int, int, float64, error) {
	if t == nil {
		return 0, 0, 0, nil
	}
	t.Lock()
	defer t.Unlock()
	sum, ok := t.data.SumData[propName]
	if !ok {
		return 0, 0, 0, nil // Required to match the old prop tracker (for now)
	}
	count, ok := t.data.CountData[propName]
	if !ok {
		return 0, 0, 0, nil // Required to match the old prop tracker (for now)
	}
	return sum, count, float64(sum) / float64(count), nil
}

// Returns the number of documents stored in the shard
func (t *JsonShardMetaData) ObjectTally() int {
	if t == nil {
		return 0
	}
	t.Lock()
	defer t.Unlock()

	return t.data.ObjectCount
}

// Writes the current state of the tracker to disk.  (flushBackup = true) will only write the backup file
func (t *JsonShardMetaData) Flush(flushBackup bool) error {
	if t == nil {
		return nil
	}
	if !flushBackup { // Write the backup file first
		t.Flush(true)
	}

	t.Lock()
	defer t.Unlock()

	bytes, err := json.Marshal(t.data)
	if err != nil {
		return err
	}

	filename := t.path
	if flushBackup {
		filename = t.path + ".bak"
	}

	// Do a write+rename to avoid corrupting the file if we crash while writing
	tempfile := filename + ".tmp"

	err = os.WriteFile(tempfile, bytes, 0o666)
	if err != nil {
		return err
	}

	err = os.Rename(tempfile, filename)
	if err != nil {
		return err
	}

	return nil
}

// Closes the tracker and removes the backup file
func (t *JsonShardMetaData) Close() error {
	if t == nil {
		return nil
	}
	if err := t.Flush(false); err != nil {
		return errors.Wrap(err, "flush before closing")
	}

	t.Lock()
	defer t.Unlock()

	t.data.BucketedData = nil

	return nil
}

// Drop removes the tracker from disk
func (t *JsonShardMetaData) Drop() error {
	if t == nil {
		return nil
	}
	t.Close()

	t.Lock()
	defer t.Unlock()

	t.data.BucketedData = nil

	if err := os.Remove(t.path); err != nil {
		return errors.Wrap(err, "remove prop length tracker state from disk:"+t.path)
	}
	if err := os.Remove(t.path + ".bak"); err != nil {
		return errors.Wrap(err, "remove prop length tracker state from disk:"+t.path+".bak")
	}

	return nil
}
