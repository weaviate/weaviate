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
func ScanObjectsLSM(store *lsmkv.Store, pointers []uint64, scan ObjectScanFn, properties []string) error {
	return newObjectScannerLSM(store, pointers, scan, properties).Do()
}

type objectScannerLSM struct {
	store         *lsmkv.Store
	pointers      []uint64
	scanFn        ObjectScanFn
	objectsBucket *lsmkv.Bucket
	properties    []string
}

func newObjectScannerLSM(store *lsmkv.Store, pointers []uint64,
	scan ObjectScanFn, properties []string,
) *objectScannerLSM {
	return &objectScannerLSM{
		store:      store,
		pointers:   pointers,
		scanFn:     scan,
		properties: properties,
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

	// Preallocate strings needed for json unmarshalling
	propStrings := make([][]string, len(os.properties))
	for i := range os.properties {
		propStrings[i] = []string{os.properties[i]}
	}

	// The typed properties are needed for extraction from json
	var properties models.PropertySchema
	propertiesTyped := map[string]interface{}{}

	for _, prop := range os.properties {
		propertiesTyped[prop] = nil
	}

	for _, id := range os.pointers {
		binary.LittleEndian.PutUint64(docIDBytes, id)
		res, err := os.objectsBucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return err
		}

		if res == nil {
			continue
		}

		if len(os.properties) > 0 {
			err = storobj.UnmarshalPropertiesFromObject(res, &propertiesTyped, os.properties, propStrings)
			if err != nil {
				return errors.Wrapf(err, "unmarshal data object")
			}
			properties = propertiesTyped
		}

		continueScan, err := os.scanFn(&properties, id)
		if err != nil {
			return errors.Wrapf(err, "scan")
		}

		if !continueScan {
			break
		}
	}

	return nil
}
