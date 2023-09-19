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

/*
Proxy Buckets wrap the current buckets and modify the key value as requests are passed through, adding the property id as a prefix.

This is the core part of the effort to merge property buckets into 2 buckets.  At the moment, each property can have around 5 bucket files: index, filter, length, timestamp and probably more I don't remember.  This causes issues for the operating system when there are many properties, as the OS can only have a certain number of open files.  Therefore, we want to merge all the properties into 2 buckets: one for the index and one for filters.

However, the original bucket class needs to continue operating as before, because there are still parts of the code that do not need to be merged.

In order to accommodate this, we need to modify the keys of the requests as they access the bucket.  This is what the proxy bucket does.  It takes a legacy bucket and a property name, and then it intercepts calls to the bucket, modifies the keys of the requests to add the property id as a prefix to every call, and then passes the request on to the legacy bucket.

Then we have to go through the code and alter every line that creates or loads a property bucket, and wrap it with a proxy bucket (and a property name).  Then the rest of the code will perform identical calls to the proxy bucket as it would to the real bucket, but the keys will be quietly modified.

From now on, all property data MUST go through a proxy bucket. e.g.

    	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
		if err != nil {
			t.Fatalf("Failed to create bucket: %v", err)
		}

		bp := WrapBucketWithProp(b, propName, propids)

In order to speed up access, the property name is not used as a prefix, instead each property is given a number and that is used as a prefix.  The property name is only used for debugging.

*/

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
	realBucket     BucketInterface // the real storage bucket
	propertyPrefix []byte          // the property id as a byte prefix, only keys with this prefix will be  accessed
	PropertyName   string          // the property name, used mainly for debugging
}

var UseMergedBuckets = true // if true, use the merged buckets, if false, use the legacy buckets

//Wraps a bucket with a property prefixer
func WrapBucketWithProp(bucket *Bucket, propName string, propIds *tracker.JsonPropertyIdTracker) (BucketInterface, error) {
	bucketValue, err := NewBucketProxy(bucket, propName, propIds)
	if err != nil {
		return nil, fmt.Errorf("cannot wrap raw bucket with property prefixer '%s': %v", propName, err)
	}

	return bucketValue, nil
}

//Returns either a real bucket or a proxy bucket, depending on whether the merged bucket feature is active or not
func FetchMeABucket(store *Store, mergedName string, propName string, propids *tracker.JsonPropertyIdTracker) (BucketInterface, error) {
	bucket := store.Bucket(mergedName)
	if UseMergedBuckets {
		return bucket, nil
	}
	proxyBucket, err := NewBucketProxy(bucket, propName, propids)
	if err != nil {
		return nil, fmt.Errorf("could not create proxy bucket for prop %s: %v", propName, err)
	}
	return proxyBucket, nil
}


func NewBucketProxy(realB BucketInterface, propName string, propids *tracker.JsonPropertyIdTracker) (*BucketProxy, error) {
	if propids == nil {
		return nil, fmt.Errorf("propids is nil")
	}
	propid_bytes := helpers.MakeByteEncodedPropertyPostfix(propName, propids)

	return &BucketProxy{
		realBucket:     realB,
		propertyPrefix: propid_bytes,
		PropertyName:   propName,
	}, nil
}

func (b *BucketProxy) PropertyPrefix() []byte {
	return b.propertyPrefix
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
	return helpers.MakePropertyKey(b.propertyPrefix, key)
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
