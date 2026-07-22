//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package common

import (
	"sync"
	"testing"
)

func TestPagedBitset(t *testing.T) {
	bs := NewPagedBitset(16)

	if !bs.TryAdd(42) {
		t.Fatal("expected first add to return true")
	}
	if !bs.Contains(42) {
		t.Fatal("expected added ID to be present")
	}
	if bs.TryAdd(42) {
		t.Fatal("expected duplicate add to return false")
	}
	if bs.LivePages() != 1 {
		t.Fatalf("expected 1 live page, got %d", bs.LivePages())
	}

	if !bs.Delete(42) {
		t.Fatal("expected delete to remove present ID")
	}
	if bs.Contains(42) {
		t.Fatal("expected removed ID to be absent")
	}
	if bs.Delete(42) {
		t.Fatal("expected delete on absent ID to return false")
	}
	if bs.LivePages() != 0 {
		t.Fatalf("expected page to be released, got %d live pages", bs.LivePages())
	}

	if !bs.TryAdd(42) {
		t.Fatal("expected re-add after page deletion to return true")
	}
	if !bs.Contains(42) {
		t.Fatal("expected re-added ID to be present")
	}
}

func TestPagedBitsetReleasesOnlyEmptyPage(t *testing.T) {
	bs := NewPagedBitset(16)

	firstPageID := uint64(1)
	first := firstPageID << pagedBitsetPageBits
	second := first + 1
	otherPage := uint64(2) << pagedBitsetPageBits

	if !bs.TryAdd(first) || !bs.TryAdd(second) || !bs.TryAdd(otherPage) {
		t.Fatal("expected all first adds to return true")
	}
	if bs.LivePages() != 2 {
		t.Fatalf("expected 2 live pages, got %d", bs.LivePages())
	}

	if !bs.Delete(first) {
		t.Fatal("expected first ID to be removed")
	}
	if bs.LivePages() != 2 {
		t.Fatalf("expected page to remain live, got %d live pages", bs.LivePages())
	}

	if !bs.Delete(second) {
		t.Fatal("expected second ID to be removed")
	}
	if bs.LivePages() != 1 {
		t.Fatalf("expected empty page to be released, got %d live pages", bs.LivePages())
	}
	if !bs.Contains(otherPage) {
		t.Fatal("expected other page ID to remain present")
	}
}

func TestPagedBitsetSupportsHighIDs(t *testing.T) {
	bs := NewPagedBitset(1 << 16)
	id := uint64(1 << 30)

	if !bs.TryAdd(id) {
		t.Fatal("expected high ID add to return true")
	}
	if !bs.Contains(id) {
		t.Fatal("expected high ID to be present")
	}
	if !bs.Delete(id) {
		t.Fatal("expected high ID delete to return true")
	}
	if bs.LivePages() != 0 {
		t.Fatalf("expected high ID page to be released, got %d live pages", bs.LivePages())
	}
}

func TestPagedBitsetConcurrentDenseAddDelete(t *testing.T) {
	bs := NewPagedBitset(64)

	const prefill = 1_000_000
	const inserts = 1_000_000
	const deletes = 500_000
	const workers = 16

	for id := uint64(0); id < prefill; id++ {
		if !bs.TryAdd(id) {
			t.Fatalf("expected prefill add %d to return true", id)
		}
	}

	var wg sync.WaitGroup
	wg.Add(workers * 2)

	for workerID := 0; workerID < workers; workerID++ {
		workerID := workerID
		go func() {
			defer wg.Done()
			for n := uint64(workerID); n < inserts; n += workers {
				bs.TryAdd(prefill + n)
			}
		}()

		go func() {
			defer wg.Done()
			for id := uint64(workerID); id < deletes; id += workers {
				bs.Delete(id)
			}
		}()
	}

	wg.Wait()

	for id := uint64(0); id < deletes; id++ {
		if bs.Contains(id) {
			t.Fatalf("expected deleted ID %d to be absent", id)
		}
	}
	for id := uint64(deletes); id < prefill+inserts; id++ {
		if !bs.Contains(id) {
			t.Fatalf("expected ID %d to be present", id)
		}
	}
}
