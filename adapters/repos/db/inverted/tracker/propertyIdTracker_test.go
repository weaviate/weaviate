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

package tracker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestJsonPropertyIdTracker(t *testing.T) {
	path := t.TempDir() + "/test.json"
	defer os.Remove(path)
	defer os.Remove(path + ".bak")

	t.Run("NewJsonPropertyIdTracker", func(t *testing.T) {
		tracker, err := NewJsonPropertyIdTracker(path)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if tracker.LastId != 1 {
			t.Fatalf("expected LastId 1, got %v", tracker.LastId)
		}
		tracker.Drop()
	})

	t.Run("Flush", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		err := tracker.Flush(false)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		_, err = os.Stat(path)
		if os.IsNotExist(err) {
			t.Fatalf("expected file to exist")
		}
		tracker.Drop()
	})

	t.Run("FlushBackup", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		err := tracker.Flush(true)
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		_, err = os.Stat(path + ".bak")
		if os.IsNotExist(err) {
			t.Fatalf("expected backup file to exist")
		}
		tracker.Drop()
	})

	t.Run("Close", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)

		tracker.Drop()
	})

	t.Run("Drop", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		err := tracker.Drop()
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		_, err = os.Stat(path)
		if !os.IsNotExist(err) {
			t.Fatalf("expected file to not exist")
		}
	})

	t.Run("GetIdForProperty", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		_, err := tracker.GetIdForProperty("test")
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		tracker.Drop()
	})

	t.Run("CreateProperty", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		id, err := tracker.CreateProperty("test")
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}

		fileBytes, _ := os.ReadFile(path)
		fileContents := &JsonPropertyIdTracker{}
		json.Unmarshal(fileBytes, fileContents)

		if fileContents.PropertyIds["test"] != id {
			t.Fatalf("expected property id to match, got %v and %v", fileContents.PropertyIds["test"], id)
		}
		tracker.Drop()
	})

	t.Run("CreateExistingProperty", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		tracker.CreateProperty("test")
		_, err := tracker.CreateProperty("test")
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
		tracker.Drop()
	})
}

func TestJsonPropertyIdTracker_ConcurrentAccess(t *testing.T) {
	path := t.TempDir() + "/test.json"
	defer os.Remove(path)
	defer os.Remove(path + ".bak")

	t.Run("ConcurrentCreateProperty", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		defer tracker.Drop()

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				property := fmt.Sprintf("property%v", i)
				_, err := tracker.CreateProperty(property)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}(i)
		}
		wg.Wait()

		if len(tracker.PropertyIds) != 100 {
			t.Fatalf("expected 100 properties, got %v", len(tracker.PropertyIds))
		}
		tracker.Drop()
	})

	t.Run("ConcurrentGetIdForProperty", func(t *testing.T) {
		tracker, _ := NewJsonPropertyIdTracker(path)
		defer tracker.Drop()

		property := "concurrentProperty"
		tracker.CreateProperty(property)

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := tracker.GetIdForProperty(property)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()
		tracker.Drop()
	})
}

func TestJsonPropertyIdTracker_ConcurrentOpenClose(t *testing.T) {
	path := t.TempDir() + "/test.json"
	defer os.Remove(path)
	defer os.Remove(path + ".bak")

	t.Run("ConcurrentOpen", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := NewJsonPropertyIdTracker(path)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()
	})
}
