//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package docid

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
)

// ObjectsinTx resolves all the specified pointers to actual objects.  This
// should only be called if the intent is to return those objects to the user,
// as we need enough Memory to hold all of those objects. Do not use this in
// whole-db-scan situations such as an aggregation where you might only need
// each object for a short amount of time to extract one or more properties. In
// that case use ScanObjectsInTx directly
func ObjectsInTx(tx *bolt.Tx,
	pointers []uint64) ([]*storobj.Object, error) {
	// at most the resulting array can have the length of the input pointers,
	// however it could also be smaller if one (or more) of the pointers resolve
	// to nil-ids or nil-objects
	out := make([]*storobj.Object, len(pointers))

	i := 0
	err := ScanObjectsInTx(tx, pointers,
		func(obj *storobj.Object) (bool, error) {
			out[i] = obj
			i++
			return true, nil
		})
	if err != nil {
		return nil, err
	}

	return out[:i], nil
}

// ObjectScanFn is called once per object, if false or an error is returned,
// the scanning will stop
type ObjectScanFn func(obj *storobj.Object) (bool, error)

// ScanObjectsInTx calls the provided scanFn on each object for the
// specified pointer. If a pointer does not resolve to an object-id, the item
// will be skipped. The number of times scanFn is called can therefore be
// smaller than the input length of pointers.
func ScanObjectsInTx(tx *bolt.Tx, pointers []uint64, scan ObjectScanFn) error {
	// TODO: should this have a ctx?
	return newObjectScanner(tx, pointers, scan).Do()
}

type objectScanner struct {
	tx            *bolt.Tx
	pointers      []uint64
	scanFn        ObjectScanFn
	lookupBucket  *bolt.Bucket
	objectsBucket *bolt.Bucket
}

func newObjectScanner(tx *bolt.Tx, pointers []uint64,
	scan ObjectScanFn) *objectScanner {
	return &objectScanner{
		tx:       tx,
		pointers: pointers,
		scanFn:   scan,
	}
}

func (os *objectScanner) Do() error {
	if err := os.init(); err != nil {
		return errors.Wrap(err, "init object scanner")
	}

	uuidKeys, err := os.resolveDocIDs()
	if err != nil {
		return errors.Wrap(err, "resolve docid pointers")
	}

	if err := os.scan(uuidKeys); err != nil {
		return errors.Wrap(err, "scan")
	}

	return nil
}

func (os *objectScanner) init() error {
	lookup := os.tx.Bucket(helpers.DocIDBucket)
	if lookup == nil {
		return fmt.Errorf("doc id lookup bucket not found")
	}
	os.lookupBucket = lookup

	objects := os.tx.Bucket(helpers.ObjectsBucket)
	if objects == nil {
		return fmt.Errorf("objects bucket not found")
	}
	os.objectsBucket = objects

	return nil
}

func (os *objectScanner) resolveDocIDs() ([][]byte, error) {
	uuidKeys := make([][]byte, len(os.pointers))

	uuidIndex := 0
	for _, pointer := range os.pointers {
		keyBuf := bytes.NewBuffer(nil)
		pointerUint64 := uint64(pointer)
		binary.Write(keyBuf, binary.LittleEndian, &pointerUint64)
		key := keyBuf.Bytes()
		docIDLookup := os.lookupBucket.Get(key)
		if len(docIDLookup) == 0 {
			// we received a doc id that was marked as deleted and cleaned up
			continue
		}

		lookup, err := LookupFromBinary(docIDLookup)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal doc id lookup")
		}

		if lookup.Deleted {
			// marked as deleted, but not cleaned up yet, skip
			continue
		}

		uuidKeys[uuidIndex] = lookup.PointsTo
		uuidIndex++
	}

	return uuidKeys[:uuidIndex], nil
}

func (os *objectScanner) scan(uuidKeys [][]byte) error {
	for i, uuid := range uuidKeys {
		elem, err := storobj.FromBinary(os.objectsBucket.Get(uuid))
		if err != nil {
			return errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		continueScan, err := os.scanFn(elem)
		if err != nil {
			return errors.Wrapf(err, "scanFn at position %d", i)
		}

		if !continueScan {
			break
		}
	}

	return nil
}
