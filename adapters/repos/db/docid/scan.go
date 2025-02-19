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

package docid

import (
	"encoding/binary"
	"math"
	"runtime"
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

func newObjectScannerLSM(store *lsmkv.Store, pointers []uint64, scan ObjectScanFn, properties []string, logger logrus.FieldLogger) *objectScannerLSM {
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
	// each object is scanned one after the other, so we can reuse the same memory allocations for all objects
	docIDBytes := make([]byte, 8)

	// Preallocate property paths needed for json unmarshalling
	propertyPaths := make([][]string, len(os.properties))
	for i := range os.properties {
		propertyPaths[i] = []string{os.properties[i]}
	}

	concurrency := 2 * runtime.GOMAXPROCS(0)

	var continueScan atomic.Bool
	continueScan.Store(true)

	type result struct {
		properties *models.PropertySchema
		docID      uint64
	}

	resultsCh := make(chan result, concurrency*2)
	var err error
	go func() {
		for res := range resultsCh {
			sc, e := os.scanFn(res.properties, res.docID)
			if e != nil || !sc {
				err = e
				continueScan.Store(false)
				return
			}
		}
	}()

	eg := enterrors.NewErrorGroupWrapper(os.logger)
	stride := int(math.Ceil(max(float64(len(os.pointers))/float64(concurrency), 1)))

	for workerID := range concurrency {
		workerID := workerID
		eg.Go(func() error {
			if !continueScan.Load() {
				return nil
			}

			var (
				start = workerID * stride
				end   = min(start+stride, len(os.pointers))
			)

			if start >= len(os.pointers) {
				return nil
			}

			for _, id := range os.pointers[start:end] {
				binary.LittleEndian.PutUint64(docIDBytes, id)
				res, err := os.objectsBucket.GetBySecondaryV2(0, docIDBytes)
				if err != nil {
					return err
				}

				if res.Bytes == nil {
					continue
				}
				var (
					// used for extraction from json
					propertiesTyped = make(map[string]interface{}, len(os.properties))
				)

				if len(propertyPaths) > 0 {
					err = storobj.UnmarshalPropertiesFromObject(res.Bytes, propertiesTyped, propertyPaths)
					if err != nil {
						return errors.Wrapf(err, "unmarshal data object")
					}
				}
				res.Release()

				schema := models.PropertySchema(propertiesTyped)
				resultsCh <- result{
					properties: &schema,
					docID:      id,
				}
			}
			return nil
		})
	}

	err2 := eg.Wait()
	if err2 != nil {
		return err2
	}
	return err
}
