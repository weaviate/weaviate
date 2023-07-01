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

	"encoding/binary"
	"fmt"
	"github.com/weaviate/weaviate/entities/storobj"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
)
type BucketProxy struct {
	realB             *Bucket
	property_prefix   []byte
	propertyIds   *tracker.JsonPropertyIdTracker
}

func NewBucketProxy(realB *Bucket, propName []byte, propids *tracker.JsonPropertyIdTracker) *BucketProxy {
	propid, err := propids.GetIdForProperty(string(propName))
	if err != nil {
		panic(fmt.Sprintf("property '%s' not found in propLengths", propName))
	}
	propid_bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(propid_bytes, propid)
	return &BucketProxy{
		realB:           realB,
		property_prefix: propid_bytes,
		propertyIds: propids,
	}
}

func (b *BucketProxy) MakePropertyKey(prefix, key []byte) []byte {
	

	t := append([]byte(b.property_prefix), byte('|'))
	val := append(t, key...)
	return val
}

func (b *BucketProxy) IterateObjects(ctx context.Context, f func(object *storobj.Object) error) error {
	return b.realB.IterateObjects(ctx, f)
}

func (b *BucketProxy) SetMemtableThreshold(size uint64) {
	b.realB.SetMemtableThreshold(size)
}

func (b *BucketProxy) Get(key []byte) ([]byte, error) {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.Get(real_key)
}

func (b *BucketProxy) GetBySecondary(pos int, key []byte) ([]byte, error) {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.GetBySecondary(pos, real_key)
}

func (b *BucketProxy) SetList(key []byte) ([][]byte, error) {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.SetList(real_key)
}

func (b *BucketProxy) Put(key, value []byte, opts ...SecondaryKeyOption) error {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.Put(real_key, value, opts...)
}

func (b *BucketProxy) SetAdd(key []byte, values [][]byte) error {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.SetAdd(real_key, values)
}

func (b *BucketProxy) SetDeleteSingle(key []byte, valueToDelete []byte) error {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.SetDeleteSingle(real_key, valueToDelete)
}

func (b *BucketProxy) WasDeleted(key []byte) (bool, error) {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.WasDeleted(real_key)
}

func (b *BucketProxy) MapList(key []byte, cfgs ...MapListOption) ([]MapPair, error) {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.MapList(real_key, cfgs...)
}

func (b *BucketProxy) MapSet(rowKey []byte, kv MapPair) error {
	real_key := b.MakePropertyKey(b.property_prefix, rowKey)
	return b.realB.MapSet(real_key, kv)
}

func (b *BucketProxy) MapDeleteKey(rowKey, mapKey []byte) error {
	real_key := b.MakePropertyKey(b.property_prefix, rowKey)
	return b.realB.MapDeleteKey(real_key, mapKey)
}

func (b *BucketProxy) Delete(key []byte, opts ...SecondaryKeyOption) error {
	real_key := b.MakePropertyKey(b.property_prefix, key)
	return b.realB.Delete(real_key, opts...)
}

func (b *BucketProxy) Count() int {
	return b.realB.Count()
}

func (b *BucketProxy) Shutdown(ctx context.Context) error {
	return b.realB.Shutdown(ctx)
}
