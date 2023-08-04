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
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestBucketProxyCreation(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	if bp == nil {
		t.Fatal("Failed to create BucketProxy")
	}

	propid_bytes, err := helpers.MakePropertyPrefix(propName, propids)
	if err != nil {
		t.Fatalf("Failed to create property prefix: %v", err)
	}
	if !bytes.Equal(bp.PropertyPrefix(), propid_bytes) {
		t.Fatalf("BucketProxy PropertyPrefix() does not match expected '%s', got '%s'", propName, bp.PropertyPrefix())
	}
}

func TestBucketProxyGetAndPut(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Put a value into the BucketProxy
	key := []byte("testKey")
	value := []byte("testValue")
	err = bp.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value into BucketProxy: %v", err)
	}

	// Retrieve the value from the BucketProxy
	retrieved, err := bp.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value from BucketProxy: %v", err)
	}

	// Check that the retrieved value matches the put value
	if string(retrieved) != string(value) {
		t.Fatalf("Retrieved value does not match put value: expected '%s', got '%s'", string(value), string(retrieved))
	}
}

func TestBucketProxyGetNotFound(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids1.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Attempt to retrieve a value that doesn't exist
	key := []byte("nonexistentKey")
	val, err := bp.Get(key)

	if !(val == nil && err == nil) {
		t.Fatalf("Expected nil value and nil error, got '%s' and '%s'", string(val), err)
	}
}

func TestBucketProxyDelete(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids2.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}
	

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Put a value into the BucketProxy
	key := []byte("testKey")
	value := []byte("testValue")
	err = bp.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value into BucketProxy: %v", err)
	}

	// Delete the value from the BucketProxy
	err = bp.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete value from BucketProxy: %v", err)
	}

	// Attempt to retrieve the deleted value
	val, err := bp.Get(key)
	if !(val == nil && err == nil) {
		t.Fatalf("Expected nil value and nil error, got '%s' and '%s'", string(val), err)
	}
}

func TestBucketProxyMapSetAndGet(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids3.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyMapCollection))
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Create a MapPair to set
	key := []byte("testKey")
	value := MapPair{Key: []byte("innerKey"), Value: []byte("innerValue")}

	// Set the MapPair into the BucketProxy
	err = bp.MapSet(key, value)
	if err != nil {
		t.Fatalf("Failed to set MapPair into BucketProxy: %v", err)
	}

	// Retrieve the MapPair from the BucketProxy
	retrieved, err := bp.MapList(key)
	if err != nil {
		t.Fatalf("Failed to get MapPair from BucketProxy: %v", err)
	}

	// Check that the retrieved MapPair matches the set MapPair
	if len(retrieved) != 1 || string(retrieved[0].Key) != string(value.Key) || string(retrieved[0].Value) != string(value.Value) {
		t.Fatalf("Retrieved MapPair does not match set MapPair: expected '%s:%s', got '%s:%s'",
			string(value.Key), string(value.Value), string(retrieved[0].Key), string(retrieved[0].Value))
	}
}

func TestBucketProxyCount(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids4.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Put a value into the BucketProxy
	key := []byte("testKey")
	value := []byte("testValue")
	err = bp.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value into BucketProxy: %v", err)
	}

	// Check the count
	count := bp.Count()
	if count != 1 {
		t.Fatalf("BucketProxy count does not match expected: expected 1, got %d", count)
	}
}
