package dataloader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

///////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////
func TestLoader(t *testing.T) {
	t.Run("test Load method", func(t *testing.T) {
		t.Parallel()
		identityLoader, _ := IDLoader(0)
		ctx := context.Background()
		future := identityLoader.Load(ctx, StringKey("1"))
		value, err := future()
		if err != nil {
			t.Error(err.Error())
		}
		if value != "1" {
			t.Error("load didn't return the right value")
		}
	})

	t.Run("test thunk does not contain race conditions", func(t *testing.T) {
		t.Parallel()
		identityLoader, _ := IDLoader(0)
		ctx := context.Background()
		future := identityLoader.Load(ctx, StringKey("1"))
		go future()
		go future()
	})

	t.Run("test Load Method Panic Safety", func(t *testing.T) {
		t.Parallel()
		defer func() {
			r := recover()
			if r != nil {
				t.Error("Panic Loader's panic should have been handled'")
			}
		}()
		panicLoader, _ := PanicLoader(0)
		ctx := context.Background()
		future := panicLoader.Load(ctx, StringKey("1"))
		_, err := future()
		if err == nil || err.Error() != "Panic received in batch function: Programming error" {
			t.Error("Panic was not propagated as an error.")
		}
	})

	t.Run("test Load Method Panic Safety in multiple keys", func(t *testing.T) {
		t.Parallel()
		defer func() {
			r := recover()
			if r != nil {
				t.Error("Panic Loader's panic should have been handled'")
			}
		}()
		panicLoader, _ := PanicLoader(0)
		futures := []Thunk{}
		ctx := context.Background()
		for i := 0; i < 3; i++ {
			futures = append(futures, panicLoader.Load(ctx, StringKey(strconv.Itoa(i))))
		}
		for _, f := range futures {
			_, err := f()
			if err == nil || err.Error() != "Panic received in batch function: Programming error" {
				t.Error("Panic was not propagated as an error.")
			}
		}
	})

	t.Run("test LoadMany returns errors", func(t *testing.T) {
		t.Parallel()
		errorLoader, _ := ErrorLoader(0)
		ctx := context.Background()
		future := errorLoader.LoadMany(ctx, Keys{StringKey("1"), StringKey("2"), StringKey("3")})
		_, err := future()
		if len(err) != 3 {
			t.Error("LoadMany didn't return right number of errors")
		}
	})

	t.Run("test LoadMany returns len(errors) == len(keys)", func(t *testing.T) {
		t.Parallel()
		loader, _ := OneErrorLoader(3)
		ctx := context.Background()
		future := loader.LoadMany(ctx, Keys{StringKey("1"), StringKey("2"), StringKey("3")})
		_, errs := future()
		if len(errs) != 3 {
			t.Errorf("LoadMany didn't return right number of errors (should match size of input)")
		}

		var errCount int = 0
		var nilCount int = 0
		for _, err := range errs {
			if err == nil {
				nilCount++
			} else {
				errCount++
			}
		}
		if errCount != 1 {
			t.Error("Expected an error on only one of the items loaded")
		}

		if nilCount != 2 {
			t.Error("Expected second and third errors to be nil")
		}
	})

	t.Run("test LoadMany returns nil []error when no errors occurred", func(t *testing.T) {
		t.Parallel()
		loader, _ := IDLoader(0)
		ctx := context.Background()
		_, err := loader.LoadMany(ctx, Keys{StringKey("1"), StringKey("2"), StringKey("3")})()
		if err != nil {
			t.Errorf("Expected LoadMany() to return nil error slice when no errors occurred")
		}
	})

	t.Run("test thunkmany does not contain race conditions", func(t *testing.T) {
		t.Parallel()
		identityLoader, _ := IDLoader(0)
		ctx := context.Background()
		future := identityLoader.LoadMany(ctx, Keys{StringKey("1"), StringKey("2"), StringKey("3")})
		go future()
		go future()
	})

	t.Run("test Load Many Method Panic Safety", func(t *testing.T) {
		t.Parallel()
		defer func() {
			r := recover()
			if r != nil {
				t.Error("Panic Loader's panic should have been handled'")
			}
		}()
		panicLoader, _ := PanicLoader(0)
		ctx := context.Background()
		future := panicLoader.LoadMany(ctx, Keys{StringKey("1")})
		_, errs := future()
		if len(errs) < 1 || errs[0].Error() != "Panic received in batch function: Programming error" {
			t.Error("Panic was not propagated as an error.")
		}
	})

	t.Run("test LoadMany method", func(t *testing.T) {
		t.Parallel()
		identityLoader, _ := IDLoader(0)
		ctx := context.Background()
		future := identityLoader.LoadMany(ctx, Keys{StringKey("1"), StringKey("2"), StringKey("3")})
		results, _ := future()
		if results[0].(string) != "1" || results[1].(string) != "2" || results[2].(string) != "3" {
			t.Error("loadmany didn't return the right value")
		}
	})

	t.Run("batches many requests", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(0)
		ctx := context.Background()
		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("2"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1", "2"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not call batchFn in right order. Expected %#v, got %#v", expected, calls)
		}
	})

	t.Run("number of results matches number of keys", func(t *testing.T) {
		t.Parallel()
		faultyLoader, _ := FaultyLoader()
		ctx := context.Background()

		n := 10
		reqs := []Thunk{}
		keys := Keys{}
		for i := 0; i < n; i++ {
			key := StringKey(strconv.Itoa(i))
			reqs = append(reqs, faultyLoader.Load(ctx, key))
			keys = append(keys, key)
		}

		for _, future := range reqs {
			_, err := future()
			if err == nil {
				t.Error("if number of results doesn't match keys, all keys should contain error")
			}
		}

		// TODO: expect to get some kind of warning
	})

	t.Run("responds to max batch size", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(2)
		ctx := context.Background()
		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("2"))
		future3 := identityLoader.Load(ctx, StringKey("3"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future3()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner1 := []string{"1", "2"}
		inner2 := []string{"3"}
		expected := [][]string{inner1, inner2}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}
	})

	t.Run("caches repeated requests", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(0)
		ctx := context.Background()
		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("1"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}
	})

	t.Run("allows primed cache", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(0)
		ctx := context.Background()
		identityLoader.Prime(ctx, StringKey("A"), "Cached")
		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("A"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		value, err := future2()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}

		if value.(string) != "Cached" {
			t.Errorf("did not use primed cache value. Expected '%#v', got '%#v'", "Cached", value)
		}
	})

	t.Run("allows clear value in cache", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(0)
		ctx := context.Background()
		identityLoader.Prime(ctx, StringKey("A"), "Cached")
		identityLoader.Prime(ctx, StringKey("B"), "B")
		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Clear(ctx, StringKey("A")).Load(ctx, StringKey("A"))
		future3 := identityLoader.Load(ctx, StringKey("B"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		value, err := future2()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future3()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1", "A"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}

		if value != "A" {
			t.Errorf("did not use primed cache value. Expected '%#v', got '%#v'", "Cached", value)
		}
	})

	t.Run("allows clearAll values in cache", func(t *testing.T) {
		t.Parallel()
		batchOnlyLoader, loadCalls := BatchOnlyLoader(0)
		ctx := context.Background()
		future1 := batchOnlyLoader.Load(ctx, StringKey("1"))
		future2 := batchOnlyLoader.Load(ctx, StringKey("1"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not batch queries. Expected %#v, got %#v", expected, calls)
		}

		if _, found := batchOnlyLoader.cache.Get(ctx, StringKey("1")); found {
			t.Errorf("did not clear cache after batch. Expected %#v, got %#v", false, found)
		}
	})

	t.Run("allows clearAll values in cache", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := IDLoader(0)
		ctx := context.Background()
		identityLoader.Prime(ctx, StringKey("A"), "Cached")
		identityLoader.Prime(ctx, StringKey("B"), "B")

		identityLoader.ClearAll()

		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("A"))
		future3 := identityLoader.Load(ctx, StringKey("B"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future3()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1", "A", "B"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}
	})

	t.Run("all methods on NoCache are Noops", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := NoCacheLoader(0)
		ctx := context.Background()
		identityLoader.Prime(ctx, StringKey("A"), "Cached")
		identityLoader.Prime(ctx, StringKey("B"), "B")

		identityLoader.ClearAll()

		future1 := identityLoader.Clear(ctx, StringKey("1")).Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("A"))
		future3 := identityLoader.Load(ctx, StringKey("B"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future3()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1", "A", "B"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}
	})

	t.Run("no cache does not cache anything", func(t *testing.T) {
		t.Parallel()
		identityLoader, loadCalls := NoCacheLoader(0)
		ctx := context.Background()
		identityLoader.Prime(ctx, StringKey("A"), "Cached")
		identityLoader.Prime(ctx, StringKey("B"), "B")

		future1 := identityLoader.Load(ctx, StringKey("1"))
		future2 := identityLoader.Load(ctx, StringKey("A"))
		future3 := identityLoader.Load(ctx, StringKey("B"))

		_, err := future1()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future2()
		if err != nil {
			t.Error(err.Error())
		}
		_, err = future3()
		if err != nil {
			t.Error(err.Error())
		}

		calls := *loadCalls
		inner := []string{"1", "A", "B"}
		expected := [][]string{inner}
		if !reflect.DeepEqual(calls, expected) {
			t.Errorf("did not respect max batch size. Expected %#v, got %#v", expected, calls)
		}
	})

}

// test helpers
func IDLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		for _, key := range keys {
			results = append(results, &Result{key.String(), nil})
		}
		return results
	}, WithBatchCapacity(max))
	return identityLoader, &loadCalls
}
func BatchOnlyLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		for _, key := range keys {
			results = append(results, &Result{key, nil})
		}
		return results
	}, WithBatchCapacity(max), WithClearCacheOnBatch())
	return identityLoader, &loadCalls
}
func ErrorLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		for _, key := range keys {
			results = append(results, &Result{key, fmt.Errorf("this is a test error")})
		}
		return results
	}, WithBatchCapacity(max))
	return identityLoader, &loadCalls
}
func OneErrorLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		results := make([]*Result, max)
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		for i := range keys {
			var err error
			if i == 0 {
				err = errors.New("always error on the first key")
			}
			results[i] = &Result{keys[i], err}
		}
		return results
	}, WithBatchCapacity(max))
	return identityLoader, &loadCalls
}
func PanicLoader(max int) (*Loader, *[][]string) {
	var loadCalls [][]string
	panicLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		panic("Programming error")
	}, WithBatchCapacity(max), withSilentLogger())
	return panicLoader, &loadCalls
}
func BadLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		results = append(results, &Result{keys[0], nil})
		return results
	}, WithBatchCapacity(max))
	return identityLoader, &loadCalls
}
func NoCacheLoader(max int) (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string
	cache := &NoCache{}
	identityLoader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()
		for _, key := range keys {
			results = append(results, &Result{key, nil})
		}
		return results
	}, WithCache(cache), WithBatchCapacity(max))
	return identityLoader, &loadCalls
}

// FaultyLoader gives len(keys)-1 results.
func FaultyLoader() (*Loader, *[][]string) {
	var mu sync.Mutex
	var loadCalls [][]string

	loader := NewBatchedLoader(func(_ context.Context, keys Keys) []*Result {
		var results []*Result
		mu.Lock()
		loadCalls = append(loadCalls, keys.Keys())
		mu.Unlock()

		lastKeyIndex := len(keys) - 1
		for i, key := range keys {
			if i == lastKeyIndex {
				break
			}

			results = append(results, &Result{key, nil})
		}
		return results
	})

	return loader, &loadCalls
}

///////////////////////////////////////////////////
// Benchmarks
///////////////////////////////////////////////////
var a = &Avg{}

func batchIdentity(_ context.Context, keys Keys) (results []*Result) {
	a.Add(len(keys))
	for _, key := range keys {
		results = append(results, &Result{key, nil})
	}
	return
}

var _ctx = context.Background()

func BenchmarkLoader(b *testing.B) {
	UserLoader := NewBatchedLoader(batchIdentity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		UserLoader.Load(_ctx, StringKey(strconv.Itoa(i)))
	}
	log.Printf("avg: %f", a.Avg())
}

type Avg struct {
	total  float64
	length float64
	lock   sync.RWMutex
}

func (a *Avg) Add(v int) {
	a.lock.Lock()
	a.total += float64(v)
	a.length++
	a.lock.Unlock()
}

func (a *Avg) Avg() float64 {
	a.lock.RLock()
	defer a.lock.RUnlock()
	if a.total == 0 {
		return 0
	} else if a.length == 0 {
		return 0
	}
	return a.total / a.length
}
