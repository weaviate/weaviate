//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package docid

import (
	"encoding/binary"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
)

// ObjectScanFn is called once per object, if false or an error is returned,
// the scanning will stop
type ObjectScanFn func(prop *models.PropertySchema, docID uint64) (bool, error)

// ScanObjectsLSM calls the provided scanFn on each object for the
// specified pointer. If a pointer does not resolve to an object-id, the item
// will be skipped. The number of times scanFn is called can therefore be
// smaller than the input length of pointers.
func ScanObjectsLSM(store *lsmkv.Store, pointers []uint64, scan ObjectScanFn, properties []string, logger logrus.FieldLogger) error {
	return newObjectScannerLSM(store, pointers, scan, properties, logger).Do()
}

type objectScannerLSM struct {
	store         *lsmkv.Store
	pointers      []uint64
	scanFn        ObjectScanFn
	objectsBucket *lsmkv.Bucket
	properties    []string
	logger        logrus.FieldLogger
}

func newObjectScannerLSM(store *lsmkv.Store, pointers []uint64,
	scan ObjectScanFn, properties []string, logger logrus.FieldLogger,
) *objectScannerLSM {
	return &objectScannerLSM{
		store:      store,
		pointers:   pointers,
		scanFn:     scan,
		properties: properties,
		logger:     logger,
	}
}

func (os *objectScannerLSM) Do() error {
	if err := os.init(); err != nil {
		return errors.Wrap(err, "init object scanner")
	}

	if err := os.scan(); err != nil {
		return errors.Wrap(err, "scan")
	}

	return nil
}

func (os *objectScannerLSM) init() error {
	bucket := os.store.Bucket(helpers.ObjectsBucketLSM)
	if bucket == nil {
		return errors.Errorf("objects bucket not found")
	}
	os.objectsBucket = bucket

	return nil
}

func (os *objectScannerLSM) scan() error {
	// Preallocate property paths needed for json unmarshalling
	propertyPaths := make([][]string, len(os.properties))
	for i := range os.properties {
		propertyPaths[i] = []string{os.properties[i]}
	}

	lock := sync.Mutex{}
	eg := enterrors.NewErrorGroupWrapper(os.logger)
	concurrency := 2 * runtime.GOMAXPROCS(0)
	stride := int(math.Ceil(max(float64(len(os.pointers))/float64(concurrency), 1)))
	contScanning := atomic.Bool{}
	contScanning.Store(true)
	for i := 0; i < concurrency; i++ {
		start := i * stride
		end := min(start+stride, len(os.pointers))
		if start >= len(os.pointers) {
			break
		}
		f := func() error {
			// each object is scanned one after the other, so we can reuse the same memory allocations for all objects
			docIDBytes := make([]byte, 8)

			// The typed properties are needed for extraction from json
			var properties models.PropertySchema
			propertiesTyped := map[string]interface{}{}

			for _, id := range os.pointers[start:end] {
				binary.LittleEndian.PutUint64(docIDBytes, id)
				res, err := os.objectsBucket.GetBySecondary(0, docIDBytes)
				if err != nil {
					return err
				}

				if res == nil {
					continue
				}

				if len(os.properties) > 0 {
					err = storobj.UnmarshalPropertiesFromObject(res, propertiesTyped, propertyPaths)
					if err != nil {
						return errors.Wrapf(err, "unmarshal data object")
					}
					properties = propertiesTyped
				}

				// majority of time is spend reading the objects => do the analyses sequentially to not cause races
				// when analysing the results
				lock.Lock()
				continueScan, err := os.scanFn(&properties, id)
				if !continueScan {
					contScanning.Store(false)
				}
				lock.Unlock()
				if err != nil {
					return errors.Wrapf(err, "scan")
				}

				if !contScanning.Load() {
					break
				}
			}
			return nil
		}

		eg.Go(f)
	}

	return eg.Wait()
}
