//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"testing"
)

func TestQueue_EnqueueAndDequeue(t *testing.T) {
	var q Queue[string]

	// Test enqueue
	q.Enqueue("first")
	q.Enqueue("second")
	q.Enqueue("third")

	if q.Size() != 3 {
		t.Errorf("Expected size 3, got %d", q.Size())
	}

	// Test dequeue order (FIFO)
	item, err := q.Dequeue()
	if err != nil || item != "first" {
		t.Errorf("Expected 'first' with no error, got %v, error: %v", item, err)
	}

	item, err = q.Dequeue()
	if err != nil || item != "second" {
		t.Errorf("Expected 'second' with no error, got %v, error: %v", item, err)
	}

	item, err = q.Dequeue()
	if err != nil || item != "third" {
		t.Errorf("Expected 'third' with no error, got %v, error: %v", item, err)
	}

	if q.Size() != 0 {
		t.Errorf("Expected size 0, got %d", q.Size())
	}
}

func TestQueue_DequeueEmptyQueue(t *testing.T) {
	var q Queue[int]

	// Test dequeue from empty queue
	item, err := q.Dequeue()
	if err == nil {
		t.Error("Expected error from empty queue dequeue")
	}

	expectedError := "tried to dequeue from an empty queue"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	// Should return zero value
	if item != 0 {
		t.Errorf("Expected zero value (0) from empty queue, got %v", item)
	}

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty")
	}
}

func TestQueue_DequeueEmptyQueueString(t *testing.T) {
	var q Queue[string]

	// Test dequeue from empty string queue
	item, err := q.Dequeue()
	if err == nil {
		t.Error("Expected error from empty queue dequeue")
	}

	// Should return zero value for string (empty string)
	if item != "" {
		t.Errorf("Expected empty string from empty queue, got '%s'", item)
	}
}

func TestQueue_Reset(t *testing.T) {
	var q Queue[int]

	// Add some items
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)

	if q.Size() != 3 {
		t.Errorf("Expected size 3, got %d", q.Size())
	}

	// Reset queue
	q.Reset()

	if q.Size() != 0 {
		t.Errorf("Expected size 0 after reset, got %d", q.Size())
	}

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty after reset")
	}

	// Test dequeue after reset
	item, err := q.Dequeue()
	if err == nil {
		t.Error("Expected error from empty queue after reset")
	}

	if item != 0 {
		t.Errorf("Expected zero value from reset queue, got %v", item)
	}
}

func TestQueue_IsEmpty(t *testing.T) {
	var q Queue[string]

	// Initially empty
	if !q.IsEmpty() {
		t.Error("Expected new queue to be empty")
	}

	// Add item
	q.Enqueue("test")
	if q.IsEmpty() {
		t.Error("Expected queue with item to not be empty")
	}

	// Remove item
	item, err := q.Dequeue()
	if err != nil || item != "test" {
		t.Errorf("Expected 'test' with no error, got %v, error: %v", item, err)
	}

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty after removing all items")
	}
}

func TestQueue_Size(t *testing.T) {
	var q Queue[int]

	// Initially size 0
	if q.Size() != 0 {
		t.Errorf("Expected size 0, got %d", q.Size())
	}

	// Add items and check size
	for i := 1; i <= 5; i++ {
		q.Enqueue(i)
		if q.Size() != i {
			t.Errorf("Expected size %d, got %d", i, q.Size())
		}
	}

	// Remove items and check size
	for i := 4; i >= -1; i-- {
		_, err := q.Dequeue()
		if i >= 0 && err != nil {
			t.Errorf("Unexpected error during dequeue: %v", err)
		} else if i == -1 && err == nil {
			t.Error("Expected error when dequeuing from empty queue")
		}

		if i >= 0 && q.Size() != i {
			t.Errorf("Expected size %d, got %d", i, q.Size())
		}
	}
}

func TestQueue_WithDifferentTypes(t *testing.T) {
	// Test with integers
	var intQueue Queue[int]
	intQueue.Enqueue(42)
	intQueue.Enqueue(100)

	item, err := intQueue.Dequeue()
	if err != nil || item != 42 {
		t.Errorf("Expected 42 with no error, got %v, error: %v", item, err)
	}

	// Test with custom struct
	type Person struct {
		Name string
		Age  int
	}

	var personQueue Queue[Person]
	person1 := Person{Name: "Alice", Age: 30}
	person2 := Person{Name: "Bob", Age: 25}

	personQueue.Enqueue(person1)
	personQueue.Enqueue(person2)

	result, err := personQueue.Dequeue()
	if err != nil || result.Name != "Alice" || result.Age != 30 {
		t.Errorf("Expected Alice(30) with no error, got %v, error: %v", result, err)
	}

	result, err = personQueue.Dequeue()
	if err != nil || result.Name != "Bob" || result.Age != 25 {
		t.Errorf("Expected Bob(25) with no error, got %v, error: %v", result, err)
	}

	// Test empty struct queue
	result, err = personQueue.Dequeue()
	if err == nil {
		t.Error("Expected error from empty struct queue")
	}

	// Should return zero value for struct
	expectedZero := Person{Name: "", Age: 0}
	if result != expectedZero {
		t.Errorf("Expected zero value %v from empty queue, got %v", expectedZero, result)
	}
}

func TestQueue_FIFOBehavior(t *testing.T) {
	var q Queue[string]

	// Add multiple items
	items := []string{"a", "b", "c", "d", "e"}
	for _, item := range items {
		q.Enqueue(item)
	}

	// Verify FIFO order
	for i, expected := range items {
		result, err := q.Dequeue()
		if err != nil || result != expected {
			t.Errorf("At position %d: expected %s with no error, got %v, error: %v", i, expected, result, err)
		}
	}

	// Queue should be empty now
	if !q.IsEmpty() {
		t.Error("Expected queue to be empty after dequeuing all items")
	}
}

func TestQueue_MixedOperations(t *testing.T) {
	var q Queue[int]

	// Mix enqueue and dequeue operations
	q.Enqueue(1)
	q.Enqueue(2)

	item, err := q.Dequeue()
	if err != nil || item != 1 {
		t.Errorf("Expected 1 with no error, got %v, error: %v", item, err)
	}

	q.Enqueue(3)
	q.Enqueue(4)

	if q.Size() != 3 { // 2, 3, 4
		t.Errorf("Expected size 3, got %d", q.Size())
	}

	item, err = q.Dequeue()
	if err != nil || item != 2 {
		t.Errorf("Expected 2 with no error, got %v, error: %v", item, err)
	}

	item, err = q.Dequeue()
	if err != nil || item != 3 {
		t.Errorf("Expected 3 with no error, got %v, error: %v", item, err)
	}

	item, err = q.Dequeue()
	if err != nil || item != 4 {
		t.Errorf("Expected 4 with no error, got %v, error: %v", item, err)
	}

	if !q.IsEmpty() {
		t.Error("Expected queue to be empty")
	}
}

func TestQueue_ErrorHandling(t *testing.T) {
	var q Queue[uint64]

	// Test multiple dequeues from empty queue
	for i := 0; i < 3; i++ {
		item, err := q.Dequeue()
		if err == nil {
			t.Errorf("Attempt %d: Expected error from empty queue", i+1)
		}

		if item != 0 {
			t.Errorf("Attempt %d: Expected zero value (0) from empty queue, got %v", i+1, item)
		}
	}

	// Add item and dequeue successfully
	q.Enqueue(42)
	item, err := q.Dequeue()
	if err != nil || item != 42 {
		t.Errorf("Expected 42 with no error, got %v, error: %v", item, err)
	}

	// Should be empty again
	_, err = q.Dequeue()
	if err == nil {
		t.Error("Expected error after dequeuing all items")
	}
}

func TestQueue_ZeroValueBehavior(t *testing.T) {
	// Test with pointer type
	var ptrQueue Queue[*int]

	item, err := ptrQueue.Dequeue()
	if err == nil {
		t.Error("Expected error from empty pointer queue")
	}

	if item != nil {
		t.Errorf("Expected nil from empty pointer queue, got %v", item)
	}

	// Test with slice type
	var sliceQueue Queue[[]string]

	sliceItem, err := sliceQueue.Dequeue()
	if err == nil {
		t.Error("Expected error from empty slice queue")
	}

	if sliceItem != nil {
		t.Errorf("Expected nil slice from empty queue, got %v", sliceItem)
	}
}
