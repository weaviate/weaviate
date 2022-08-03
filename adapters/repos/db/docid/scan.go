//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package docid

import (
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/lsmkv"
	"github.com/semi-technologies/weaviate/entities/storobj"
)

// ObjectScanFn is called once per object, if false or an error is returned,
// the scanning will stop
type ObjectScanFn func(obj *storobj.Object) (bool, error)

// ScanObjectsLSM calls the provided scanFn on each object for the
// specified pointer. If a pointer does not resolve to an object-id, the item
// will be skipped. The number of times scanFn is called can therefore be
// smaller than the input length of pointers.
func ScanObjectsLSM(store *lsmkv.Store, pointers []uint64, scan ObjectScanFn) error {
	return newObjectScannerLSM(store, pointers, scan).Do()
}

type objectScannerLSM struct {
	store         *lsmkv.Store
	pointers      []uint64
	scanFn        ObjectScanFn
	objectsBucket *lsmkv.Bucket
}

func newObjectScannerLSM(store *lsmkv.Store, pointers []uint64,
	scan ObjectScanFn,
) *objectScannerLSM {
	return &objectScannerLSM{
		store:    store,
		pointers: pointers,
		scanFn:   scan,
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
	docIDBytes := make([]byte, 8)
	for _, id := range os.pointers {
		binary.LittleEndian.PutUint64(docIDBytes, id)
		res, err := os.objectsBucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return err
		}

		if res == nil {
			continue
		}

		elem, err := storobj.FromBinary(res)
		if err != nil {
			return errors.Wrapf(err, "unmarshal data object")
		}

		continueScan, err := os.scanFn(elem)
		if err != nil {
			return errors.Wrapf(err, "scan")
		}

		if !continueScan {
			break
		}
	}

	return nil
}
