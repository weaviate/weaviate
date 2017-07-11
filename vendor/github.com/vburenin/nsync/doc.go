// nsync package provides a set of primitives that are not provided by standard Go library.
//
// NamedMutex - a map of dynamically created mutexes by a referred name.
//
// OnceMutex - very similar to sync.Once, however, it is a mutex not a function wrapper.
//
// NamedOnceMutex - a map of dynamically created mutexes that can be acquired only once.
// However, once mutex unlocked it is removed from the map.
// So, next attempt to acquire this mutex will succeed.
//
// Semaphore - a semaphore primitive that can be acquired limited number of times.
//
// TryMutex - A mutex that provide ability to set a timeout to acquire a lock.
//
// ControlWaitGroup - a controlled goroutine executor that can limit the number concurrently running
// goroutines. Can help to solve a resource exhaustion problem.
package nsync
