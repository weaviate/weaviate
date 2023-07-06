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

package lsmkv

import (
	"github.com/pkg/errors"

	"context"

	"encoding/binary"
	"fmt"

	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
)

type BucketProxy struct {
	realB           BucketInterface
	property_prefix []byte
	propertyIds     *tracker.JsonPropertyIdTracker
}

func NewBucketProxy(realB BucketInterface, propName []byte, propids *tracker.JsonPropertyIdTracker) *BucketProxy {
	propid, err := propids.GetIdForProperty(string(propName))
	if err != nil {
		fmt.Print(fmt.Sprintf("property '%s' not found in propLengths", propName))
	}
	propid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propid_bytes, propid)
	return &BucketProxy{
		realB:           realB,
		property_prefix: propid_bytes,
		propertyIds:     propids,
	}
}

func (b *BucketProxy) PropertyPrefix() []byte {
	return b.property_prefix
}

func (b *BucketProxy) makePropertyKey(key []byte) []byte {
	return helpers.MakePropertyKey([]byte(b.property_prefix), key)
}

func (b *BucketProxy) CursorRoaringSet() CursorRoaringSet {
	return b.realB.CursorRoaringSet()
}

func (b *BucketProxy) CursorRoaringSetKeyOnly() CursorRoaringSet {
	return b.realB.CursorRoaringSetKeyOnly()
}

func (b *BucketProxy) MapCursorKeyOnly(cfgs ...MapListOption) *CursorMap {
	return b.realB.MapCursorKeyOnly(cfgs...)
}

func (b *BucketProxy) MapCursor(cfgs ...MapListOption) *CursorMap {
	return b.realB.MapCursor(cfgs...)
}

func (b *BucketProxy) RoaringSetGet(key []byte) (*sroar.Bitmap, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.RoaringSetGet(real_key)
}

func (b *BucketProxy) SetCursor() *CursorSet {
	return b.realB.SetCursor()
}

func (b *BucketProxy) SetCursorKeyOnly() *CursorSet {
	return b.realB.SetCursorKeyOnly()
}

func (b *BucketProxy) Strategy() string {
	return b.realB.Strategy()
}

func (b *BucketProxy) IterateObjects(ctx context.Context, f func(object *storobj.Object) error) error {
	return b.realB.IterateObjects(ctx, f)
}

func (b *BucketProxy) SetMemtableThreshold(size uint64) {
	b.realB.SetMemtableThreshold(size)
}

func (b *BucketProxy) Get(key []byte) ([]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.Get(real_key)
}

func (b *BucketProxy) GetBySecondary(pos int, key []byte) ([]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.GetBySecondary(pos, real_key)
}

func (b *BucketProxy) SetList(key []byte) ([][]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.SetList(real_key)
}

func (b *BucketProxy) Put(key, value []byte, opts ...SecondaryKeyOption) error {
	real_key := b.makePropertyKey(key)
	return b.realB.Put(real_key, value, opts...)
}

func (b *BucketProxy) SetAdd(key []byte, values [][]byte) error {
	real_key := b.makePropertyKey(key)
	return b.realB.SetAdd(real_key, values)
}

func (b *BucketProxy) SetDeleteSingle(key []byte, valueToDelete []byte) error {
	real_key := b.makePropertyKey(key)
	return b.realB.SetDeleteSingle(real_key, valueToDelete)
}

func (b *BucketProxy) WasDeleted(key []byte) (bool, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.WasDeleted(real_key)
}

func (b *BucketProxy) MapList(key []byte, cfgs ...MapListOption) ([]MapPair, error) {
	real_key := b.makePropertyKey(key)
	return b.realB.MapList(real_key, cfgs...)
}

func (b *BucketProxy) MapSet(rowKey []byte, kv MapPair) error {
	real_key := b.makePropertyKey(rowKey)
	return b.realB.MapSet(real_key, kv)
}

func (b *BucketProxy) MapDeleteKey(rowKey, mapKey []byte) error {
	real_key := b.makePropertyKey(rowKey)
	return b.realB.MapDeleteKey(real_key, mapKey)
}

func (b *BucketProxy) Delete(key []byte, opts ...SecondaryKeyOption) error {
	real_key := b.makePropertyKey(key)
	return b.realB.Delete(real_key, opts...)
}

func (b *BucketProxy) Count() int {
	return b.realB.Count()
}

func (b *BucketProxy) Shutdown(ctx context.Context) error {
	return b.realB.Shutdown(ctx)
}

func (b *BucketProxy) FlushAndSwitch() error {
	return b.realB.FlushAndSwitch()
}

func (b *BucketProxy) RoaringSetAddOne(key []byte, value uint64) error {
	real_key := b.makePropertyKey(key)
	return b.realB.RoaringSetAddOne(real_key, value)
}

func (b *BucketProxy) Cursor() *CursorReplace {
	return b.realB.Cursor()
}

func (b *BucketProxy) IteratePropPrefixObjects(ctx context.Context, propPrefix []byte, f func(k []byte, object *storobj.Object) error, froar func(k []byte, object *sroar.Bitmap) error, fset func(k []byte, object [][]byte) error) error {

	switch b.Strategy() {
	case StrategyReplace:
		c := b.SetCursor()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if err := fset(k, v); err != nil {
				return err
			}
		}

	case StrategyMapCollection: //FIXME: Wrong type?

		i := 0
		cursor := b.Cursor()
		defer cursor.Close()

		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if !helpers.MatchesPropertyKeyPrefix(propPrefix, k) {
				continue
			}
			k = helpers.UnMakePropertyKey(propPrefix, k)
			fmt.Printf("k sans prop: %v\n", k)
			obj, err := storobj.FromBinary(v)
			if err != nil {
				return fmt.Errorf("cannot unmarshal object %d, %v", i, err)
			}
			if err := f(k, obj); err != nil {
				return errors.Wrapf(err, "callback on object '%d' failed", obj.DocID())
			}

			i++
		}

	case StrategyRoaringSet:
		c := b.CursorRoaringSet()
		defer c.Close()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !helpers.MatchesPropertyKeyPrefix(propPrefix, k) {
				continue
			}
			k = helpers.UnMakePropertyKey(propPrefix, k)
			fmt.Printf("k sans prop: %v\n", k)
			if err := froar(k, v); err != nil {
				return err
			}
		}

	}

	return nil
}
