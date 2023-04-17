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

package inverted

import (
	"encoding/json"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type PropLenData struct {
	BucketedData map[string]map[int]int
	SumData      map[string]int
	CountData    map[string]int
}

type JsonPropertyLengthTracker struct {
	path string
	data PropLenData
	sync.Mutex
	UnlimitedBuckets bool
}

// This class replaces the old PropertyLengthTracker.  It fixes a bug and provides a
// simpler, easier to maintain implementation.  The format is future-proofed, new
// data can be added to the file without breaking old versions of Weaviate.
//
// The property length tracker is used to track the length of properties in the
// inverted index.  This is inexact, each property is bucketed into one of 64
// buckets.  Each bucket has a value calculated by float32(4 * math.Pow(1.25, float64(bucket)-3.5)).
//
// The new tracker is exactly compatible with the old format to enable migration, which is why there is a -1 bucket.  Altering the number of buckets or their values will break compatibility.
func NewJsonPropertyLengthTracker(path string) (*JsonPropertyLengthTracker, error) {
	t := &JsonPropertyLengthTracker{
		data:             PropLenData{make(map[string]map[int]int), make(map[string]int), make(map[string]int)},
		path:             path,
		UnlimitedBuckets: false,
	}

	// read the file into memory
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			t.Flush(false)
			return t, nil
		}
		return nil, err
	}
	t.path = path

	var data PropLenData
	if err := json.Unmarshal(bytes, &data); err != nil {
		if bytes[0] != '{' {
			// It's probably the old format file, load the old format and convert it to the new format
			plt, err := NewPropertyLengthTracker(path)
			if err != nil {
				return nil, errors.Wrap(err, "convert old property length tracker")
			}

			propertyNames := plt.PropertyNames()
			data = PropLenData{make(map[string]map[int]int), make(map[string]int), make(map[string]int)}
			// Loop over every page and bucket in the old tracker and add it to the new tracker
			for _, name := range propertyNames {
				data.BucketedData[name] = make(map[int]int, 64)
				data.CountData[name] = 0
				data.SumData[name] = 0
				for i := 0; i <= 64; i++ {
					fromBucket := i
					if i == 64 {
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
	}
	t.data = data

	return t, nil
}

func (t *JsonPropertyLengthTracker) FileName() string {
	return t.path
}

func (t *JsonPropertyLengthTracker) TrackProperty(propName string, value float32) error {
	t.Lock()
	defer t.Unlock()

	t.data.SumData[propName] = t.data.SumData[propName] + int(value)
	t.data.CountData[propName] = t.data.CountData[propName] + 1

	bucketId := t.bucketFromValue(value)
	if _, ok := t.data.BucketedData[propName]; ok {
		t.data.BucketedData[propName][int(bucketId)] = t.data.BucketedData[propName][int(bucketId)] + 1
	} else {

		t.data.BucketedData[propName] = make(map[int]int, 64)
		t.data.BucketedData[propName][int(bucketId)] = 1
	}

	return nil
}

func (t *JsonPropertyLengthTracker) bucketFromValue(value float32) int {
	if t.UnlimitedBuckets {
		return int(value)
	}
	if value <= 5.00 {
		return int(value) - 1
	}

	bucket := int(math.Log(float64(value)/4.0)/math.Log(1.25) + 4)
	if bucket > 63 {
		return 64
	}
	return int(bucket)
}

func (t *JsonPropertyLengthTracker) valueFromBucket(bucket int) float32 {
	if t.UnlimitedBuckets {
		return float32(bucket)
	}
	if bucket <= 5 {
		return float32(bucket + 1)
	}

	if bucket > 63 {
		return -1
	}

	return float32(4 * math.Pow(1.25, float64(bucket)-3.5))
}

func (t *JsonPropertyLengthTracker) PropertyMean(propName string) (float32, error) {
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
func (t *JsonPropertyLengthTracker) PropertyTally(propName string) (int, int, float64, error) {
	t.Lock()
	defer t.Unlock()
	sum, ok := t.data.SumData[propName]
	if !ok {
		return 0, 0, 0, nil
	}
	count, ok := t.data.CountData[propName]
	if !ok {
		return 0, 0, 0, nil
	}
	return sum, count, float64(sum) / float64(count), nil
}

func (t *JsonPropertyLengthTracker) Flush(flushBackup bool) error {
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

	err = os.WriteFile(filename, bytes, 0o666)
	if err != nil {
		return err
	}
	return nil
}

func (t *JsonPropertyLengthTracker) Close() error {
	if err := t.Flush(false); err != nil {
		return errors.Wrap(err, "flush before closing")
	}

	t.Lock()
	defer t.Unlock()

	t.data.BucketedData = nil

	return nil
}

func (t *JsonPropertyLengthTracker) Drop() error {
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
