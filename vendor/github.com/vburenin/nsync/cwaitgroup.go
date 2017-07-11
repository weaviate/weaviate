package nsync

import "sync"

// ControlWaitGroup is a combination of the WaitGroup and Semaphore primitives to run goroutines.
// WaitGroup is used to insure all tasks are complete, Semaphore server a lock job to limit
// a number of concurrently running goroutines. Go is well known for its capabilities to run
// tens of thousands of goroutines, however, it is also very easy to exceed available system resources
// such as connections, opened files, etc. Thus, ControlWaitGroup fits well to limit the number
// of running goroutines.
type ControlWaitGroup struct {
	sem     *Semaphore
	wg      sync.WaitGroup
	mu      sync.Mutex
	waiting int
	abort   bool
}

// NewControlWaitGroup returns new instance of ControlWaitGroup
func NewControlWaitGroup(poolSize int) *ControlWaitGroup {
	return &ControlWaitGroup{
		sem: NewSemaphore(poolSize),
	}
}

// Do runs user defined function with no interface.
// all parameters passed to the required call should be provided
// withing a function closure.
func (cwg *ControlWaitGroup) Do(userFunc func()) bool {
	cwg.wg.Add(1)

	cwg.mu.Lock()
	cwg.waiting++
	cwg.mu.Unlock()

	cwg.sem.Acquire()
	cwg.mu.Lock()
	cwg.waiting--

	if cwg.abort {
		cwg.sem.Release()
		cwg.wg.Done()
		cwg.mu.Unlock()
		return false
	}

	cwg.mu.Unlock()

	go func() {
		defer cwg.wg.Done()
		defer cwg.sem.Release()
		userFunc()
	}()
	return true
}

// Abort interrupts execution of any pending task that is blocked by semaphore.
func (cwg *ControlWaitGroup) Abort() {
	cwg.mu.Lock()
	cwg.abort = true
	cwg.mu.Unlock()
}

// Working is a number of the currently running goroutines. This value is highly
// dynamic so it only make sense to use it for logging purposes.
func (cwg *ControlWaitGroup) Working() int {
	return cwg.sem.Value()
}

// Waiting is a number of the currently scheduled, but not running goroutines. This value is highly
// dynamic so it only make sense to use it for logging purposes.
func (cwg *ControlWaitGroup) Waiting() int {
	cwg.mu.Lock()
	w := cwg.waiting
	cwg.mu.Unlock()
	return w
}

// Wait blocks until all goroutines finish their work.
func (cwg *ControlWaitGroup) Wait() {
	cwg.wg.Wait()
}
