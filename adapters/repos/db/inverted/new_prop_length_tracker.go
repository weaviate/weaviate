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
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type PropLenData struct {
	BucketedData map[string]map[int]int
}

type JsonPropertyLengthTracker struct {
	path string
	data PropLenData
	sync.Mutex
}

func NewJsonPropertyLengthTracker(path string) (*JsonPropertyLengthTracker, error) {
	t := &JsonPropertyLengthTracker{
		data: PropLenData{make(map[string]map[int]int)},
		path: path,
	}

	// read the file into memory
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
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
			data = PropLenData{make(map[string]map[int]int, len(propertyNames))}
			// Loop over every page and bucket in the old tracker and add it to the new tracker
			for _, name := range propertyNames {
				data.BucketedData[name] = make(map[int]int, 64)
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
				}
			}
			t.data = data
			t.FlushBackup()
			plt.Close()
			plt.Drop()
			t.Flush()
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
	if bucket <= 5 {
		return float32(bucket + 1)
	}

	return float32(4 * math.Pow(1.25, float64(bucket)-3.5))
}

func (t *JsonPropertyLengthTracker) PropertyMean(propName string) (float32, error) {
	t.Lock()
	defer t.Unlock()

	bucket, ok := t.data.BucketedData[propName]
	if !ok {
		return 0, fmt.Errorf("property not found %v", propName)
	}

	sum := float32(0)
	totalCount := float32(0)

	for i := -1; i <= 64; i++ {
		count, ok := bucket[i]
		if !ok {
			count = 0
		}
		sum = sum + t.valueFromBucket(int(i))*float32(count)
		totalCount += float32(count)
	}

	if totalCount == 0 {
		return 0, nil
	}

	return sum / totalCount, nil
}

// returns totalPropertyLength, totalCount, average propertyLength = sum / totalCount, total propertylength, totalCount, error
func (t *JsonPropertyLengthTracker) PropertyTally(propName string) (int, int, float64, error) {
	t.Lock()
	defer t.Unlock()

	bucket, ok := t.data.BucketedData[propName]
	if !ok {
		return 0, 0, 0, nil
	}

	sum := int(0)
	tally := int(0)

	for i := -1; i <= 64; i++ {
		count := bucket[i]
		value := t.valueFromBucket(i)

		sum += int(value * float32(count))
		tally += int(count)
	}

	if tally == 0 {
		return 0, 0, 0, nil
	}

	return sum, tally, float64(sum) / float64(tally), nil
}

func (t *JsonPropertyLengthTracker) Flush() error {
	t.FlushBackup()
	t.Lock()
	defer t.Unlock()

	bytes, err := json.Marshal(t.data)
	if err != nil {
		return err
	}

	err = os.WriteFile(t.path, bytes, 0o666)
	if err != nil {
		return err
	}
	return nil
}

func (t *JsonPropertyLengthTracker) FlushBackup() error {
	t.Lock()
	defer t.Unlock()

	bytes, err := json.Marshal(t.data)
	if err != nil {
		return err
	}

	err = os.WriteFile(t.path+".bak", bytes, 0o666)
	if err != nil {
		return err
	}
	return nil
}

func (t *JsonPropertyLengthTracker) Close() error {
	if err := t.Flush(); err != nil {
		return errors.Wrap(err, "flush before closing")
	}

	t.Lock()
	defer t.Unlock()

	t.data.BucketedData = nil

	return nil
}

func (t *JsonPropertyLengthTracker) Drop() error {
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
