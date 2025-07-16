package common

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStack(t *testing.T) {
	var s Stack[int]

	// Initial state
	require.True(t, s.IsEmpty(), "stack should be empty initially")

	// Push one element
	s.Push(10)
	require.False(t, s.IsEmpty(), "stack should not be empty after one push")

	// Push multiple elements
	s.Push(20)
	s.Push(30)
	require.False(t, s.IsEmpty(), "stack should not be empty after multiple pushes")

	// Pop in LIFO order
	v, ok := s.Pop()
	require.True(t, ok, "expected successful pop")
	require.Equal(t, 30, v, "expected LIFO behavior")

	v, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 20, v)

	v, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 10, v)

	// Pop from empty stack
	v, ok = s.Pop()
	require.False(t, ok, "expected pop to fail on empty stack")

	// Interleaved push/pop
	s.Push(100)
	s.Push(200)
	v, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 200, v)

	s.Push(300)
	v, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 300, v)

	v, ok = s.Pop()
	require.True(t, ok)
	require.Equal(t, 100, v)

	require.True(t, s.IsEmpty(), "stack should be empty after all pops")
}

func TestStackConcurrent(t *testing.T) {
	var s Stack[int]
	const N = 1000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < N; i++ {
			s.Push(i)
		}
		wg.Done()
	}()

	go func() {
		count := 0
		for count < N {
			if _, ok := s.Pop(); ok {
				count++
			}
		}
		wg.Done()
	}()

	wg.Wait()

	if !s.IsEmpty() {
		t.Fatal("stack should be empty after concurrent push/pop")
	}
}

func TestPool(t *testing.T) {
	var p Pool[int]

	// Basic Put/Get
	p.Put(1)
	p.Put(2)
	p.Put(3)

	v1, ok1 := p.Get()
	v2, ok2 := p.Get()
	v3, ok3 := p.Get()
	require.True(t, ok1 && ok2 && ok3, "should be able to retrieve 3 values")
	require.ElementsMatch(t, []int{1, 2, 3}, []int{v1, v2, v3}, "values should match what was put in")

	// Pool should now be empty
	_, ok := p.Get()
	require.False(t, ok, "expected pool to be empty")

	// High volume put/get
	for i := 0; i < 1000; i++ {
		p.Put(i)
	}

	got := make(map[int]bool)
	for i := 0; i < 1000; i++ {
		v, ok := p.Get()
		require.True(t, ok, "expected to get value")
		got[v] = true
	}
	require.Len(t, got, 1000, "should retrieve all 1000 values uniquely")

	// Interleaved Put/Get
	for i := 0; i < 10; i++ {
		p.Put(i)
		v, ok := p.Get()
		require.True(t, ok)
		require.Equal(t, i, v)
	}

	// Concurrent Put/Get
	var wg sync.WaitGroup
	const concurrency = 100
	const perWorker = 100

	wg.Add(2 * concurrency)

	// Producers
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			for j := 0; j < perWorker; j++ {
				p.Put(id*perWorker + j)
			}
			wg.Done()
		}(i)
	}

	// Consumers
	results := make(chan int, concurrency*perWorker)
	for i := 0; i < concurrency; i++ {
		go func() {
			count := 0
			for count < perWorker {
				if v, ok := p.Get(); ok {
					results <- v
					count++
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()
	close(results)

	// Check results
	require.Len(t, results, concurrency*perWorker, "should retrieve all values without duplication")
}
