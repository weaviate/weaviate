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
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
)

type BucketProxy struct {
	realBucket      BucketInterface // the real storage bucket
	property_prefix []byte          // the property id as a byte prefix, only keys with this prefix will be  accessed
	PropertyName    string          // the property name, used mainly for debugging
}

func NewBucketProxy(realB BucketInterface, propName string, propids *tracker.JsonPropertyIdTracker) *BucketProxy {
	if propids == nil {
		panic("propids is nil")
	}
	propid_bytes, err := helpers.MakePropertyPrefix(propName, propids)
	if err != nil {
		fmt.Printf("property '%s' not found in propLengths", propName)
	}
	return NewBucketProxyWithPrefix(realB, propName, propid_bytes)
}

func NewBucketProxyWithPrefix(realB BucketInterface, propName string, propid_bytes []byte) *BucketProxy {
	return &BucketProxy{
		realBucket:      realB,
		property_prefix: propid_bytes,
		PropertyName:    propName,
	}
}

func (b *BucketProxy) PropertyPrefix() []byte {
	return b.property_prefix
}

func (b *BucketProxy) RoaringSetRemoveOne(key []byte, value uint64) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.RoaringSetRemoveOne(real_key, value)
}

func (b *BucketProxy) RoaringSetAddList(key []byte, values []uint64) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.RoaringSetAddList(real_key, values)
}

func (b *BucketProxy) makePropertyKey(key []byte) []byte {
	return helpers.MakePropertyKey(b.property_prefix, key)
}

func (b *BucketProxy) CursorRoaringSet() CursorRoaringSet {
	return b.realBucket.CursorRoaringSet()
}

func (b *BucketProxy) CursorRoaringSetKeyOnly() CursorRoaringSet {
	return b.realBucket.CursorRoaringSetKeyOnly()
}

func (b *BucketProxy) MapCursorKeyOnly(cfgs ...MapListOption) *CursorMap {
	return b.realBucket.MapCursorKeyOnly(cfgs...)
}

func (b *BucketProxy) MapCursor(cfgs ...MapListOption) *CursorMap {
	return b.realBucket.MapCursor(cfgs...)
}

func (b *BucketProxy) RoaringSetGet(key []byte) (*sroar.Bitmap, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.RoaringSetGet(real_key)
}

func (b *BucketProxy) SetCursor() *CursorSet {
	return b.realBucket.SetCursor()
}

func (b *BucketProxy) SetCursorKeyOnly() *CursorSet {
	return b.realBucket.SetCursorKeyOnly()
}

func (b *BucketProxy) Strategy() string {
	return b.realBucket.Strategy()
}

func (b *BucketProxy) IterateObjects(ctx context.Context, f func(object *storobj.Object) error) error {
	return b.realBucket.IterateObjects(ctx, f)
}

func (b *BucketProxy) SetMemtableThreshold(size uint64) {
	b.realBucket.SetMemtableThreshold(size)
}

func (b *BucketProxy) Get(key []byte) ([]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.Get(real_key)
}

func (b *BucketProxy) GetBySecondary(pos int, key []byte) ([]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.GetBySecondary(pos, real_key)
}

func (b *BucketProxy) SetList(key []byte) ([][]byte, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.SetList(real_key)
}

func (b *BucketProxy) Put(key, value []byte, opts ...SecondaryKeyOption) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.Put(real_key, value, opts...)
}

func (b *BucketProxy) SetAdd(key []byte, values [][]byte) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.SetAdd(real_key, values)
}

func (b *BucketProxy) SetDeleteSingle(key []byte, valueToDelete []byte) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.SetDeleteSingle(real_key, valueToDelete)
}

func (b *BucketProxy) WasDeleted(key []byte) (bool, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.WasDeleted(real_key)
}

func (b *BucketProxy) MapList(key []byte, cfgs ...MapListOption) ([]MapPair, error) {
	real_key := b.makePropertyKey(key)
	return b.realBucket.MapList(real_key, cfgs...)
}

func (b *BucketProxy) MapSet(rowKey []byte, kv MapPair) error {
	real_key := b.makePropertyKey(rowKey)
	return b.realBucket.MapSet(real_key, kv)
}

func (b *BucketProxy) MapDeleteKey(rowKey, mapKey []byte) error {
	real_key := b.makePropertyKey(rowKey)
	return b.realBucket.MapDeleteKey(real_key, mapKey)
}

func (b *BucketProxy) Delete(key []byte, opts ...SecondaryKeyOption) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.Delete(real_key, opts...)
}

func (b *BucketProxy) Count() int {
	return b.realBucket.Count()
}

func (b *BucketProxy) Shutdown(ctx context.Context) error {
	return b.realBucket.Shutdown(ctx)
}

func (b *BucketProxy) FlushAndSwitch() error {
	return b.realBucket.FlushAndSwitch()
}

func (b *BucketProxy) RoaringSetAddOne(key []byte, value uint64) error {
	real_key := b.makePropertyKey(key)
	return b.realBucket.RoaringSetAddOne(real_key, value)
}

func (b *BucketProxy) Cursor() *CursorReplace {
	return b.realBucket.Cursor()
}
