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

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	if bp == nil {
		t.Fatal("Failed to create BucketProxy")
	}

	propid_bytes, err := helpers.MakePropertyPrefix(propName, propids)
	if !bytes.Equal(bp.PropertyPrefix(), propid_bytes) {
		t.Fatalf("BucketProxy PropertyPrefix() does not match expected '%s', got '%s'", propName, bp.PropertyPrefix())
	}

	// ... Add other assertions as necessary
}

func TestBucketProxyGetAndPut(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids.json")

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
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

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}

	bp := NewBucketProxy(b, propName, propids)

	// Attempt to retrieve a value that doesn't exist
	key := []byte("nonexistentKey")
	_, err = bp.Get(key)

	// Here, we're expecting an error because the key doesn't exist
	if err == nil {
		t.Fatal("Expected an error when trying to get a nonexistent value, but got none")
	}

	// ... Add other assertions as necessary
}

func TestBucketProxyDelete(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids2.json")

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
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
	_, err = bp.Get(key)
	// We're expecting an error here because we deleted the key
	if err == nil {
		t.Fatal("Expected an error when trying to get a deleted key, but got none")
	}
}

func TestBucketProxyMapSetAndGet(t *testing.T) {
	tmpDir := "/tmp/lsmkvtestdir"
	os.Mkdir(tmpDir, 0o777)
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	propName := "testPropertyName"
	propids, err := tracker.NewJsonPropertyIdTracker(tmpDir + "/propids3.json")

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
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

	b, err := NewBucket(ctx, tmpDir, "", logger, nil, cyclemanager.NewNoop(), cyclemanager.NewNoop())
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
