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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/tracker"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestBucketProxyCreation(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids5.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

	if bp == nil {
		t.Fatal("Failed to create BucketProxy")
	}

	propid_bytes := helpers.MakeByteEncodedPropertyPostfix(propName, propids)

	if !bytes.Equal(bp.PropertyPrefix(), propid_bytes) {
		t.Fatalf("BucketProxy PropertyPrefix() does not match expected '%s', got '%s'", propName, bp.PropertyPrefix())
	}
}

func TestBucketProxyGetAndPut(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids6.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

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
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids1.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

	// Attempt to retrieve a value that doesn't exist
	key := []byte("nonexistentKey")
	val, err := bp.Get(key)

	if !(val == nil && err == nil) {
		t.Fatalf("Expected nil value and nil error, got '%s' and '%s'", string(val), err)
	}
}

func TestBucketProxyDelete(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids2.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

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
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids3.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), WithStrategy(StrategyMapCollection))
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

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
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids4.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp, err := NewBucketProxy(b, propName, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

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

func TestMultiplePrefixes(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName1 := "testPropertyName"
	propName2 := "mnsdakjhfklew"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/ids6.json")
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bucketProxy1, err := NewBucketProxy(b, propName1, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

	bucketProxy2, err := NewBucketProxy(b, propName2, propids)
	if err != nil {
		t.Fatalf("Failed to create BucketProxy: %v", err)
	}

	// Put the same key and different values into the BucketProxies
	key := []byte("testKey")
	value := []byte("testValue")
	value2 := []byte("testValue2")
	err = bucketProxy1.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value into BucketProxy: %v", err)
	}

	err = bucketProxy2.Put(key, value2)
	if err != nil {
		t.Fatalf("Failed to put value into BucketProxy: %v", err)
	}

	// Retrieve the value from the BucketProxy
	retrieved1, err := bucketProxy1.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value from BucketProxy: %v", err)
	}

	retrieved2, err := bucketProxy2.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value from BucketProxy: %v", err)
	}

	// Check that the retrieved value matches the put value
	if string(retrieved1) != string(value) {
		t.Fatalf("Retrieved value does not match put value: expected '%s', got '%s'", string(value), string(retrieved1))
	}

	if string(retrieved2) != string(value2) {
		t.Fatalf("Retrieved value does not match put value: expected '%s', got '%s'", string(value), string(retrieved2))
	}
}
