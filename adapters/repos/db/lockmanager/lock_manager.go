package lockmanager

import (
	"context"
	"sync"
)

var reqPool = sync.Pool{
	New: func() any {
		return &LockRequest{}
	},
}

var headPool = sync.Pool{
	New: func() any {
		return &LockHeader{
			PerID: make(map[uint64]*LockRequest),
			Cond:  sync.NewCond(new(sync.Mutex)),
		}
	},
}

// A LockManager is used to acquire locks on database objects.
type LockManager struct {
	mu sync.Mutex

	locks map[Object]*LockHeader
}

// New creates a lock manager.
func New() *LockManager {
	var lm LockManager
	lm.locks = make(map[Object]*LockHeader)
	return &lm
}

func (lm *LockManager) Lock(ctx context.Context, lockid uint64, obj *Object, mode LockMode) error {
	lm.mu.Lock()
	head, ok := lm.locks[*obj]
	if !ok {
		req := reqPool.Get().(*LockRequest)
		req.Status = LockGranted
		req.Mode = mode
		req.Lockid = lockid

		// No lock exists for this object.
		head := headPool.Get().(*LockHeader)
		head.Object = obj
		head.GroupMode = mode
		head.Queue = req
		head.Last = req
		head.Queue.Head = head
		head.PerID[lockid] = req
		lm.locks[*obj] = head
		lm.mu.Unlock()
		return nil
	}

	// A lock exists for this object.
	// Lock the queue header and unlock the map.
	head.mu.Lock()
	lm.mu.Unlock()

	// Create a new request.
	req := reqPool.Get().(*LockRequest)
	req.Head = head
	req.Mode = mode
	req.Lockid = lockid
	head.PerID[lockid] = req

	// Add the request to the queue.
	if head.Last != nil {
		head.Last.Next = req
		req.Prev = head.Last
	} else {
		head.Queue = req
	}
	head.Last = req

	// Check if the lock is compatible with the current mode and if there
	// are no other requests in the queue.
	if !head.Waiting && head.GroupMode.IsCompatibleWith(mode) {
		// No need to wait for a lock: update the group mode and return
		// immediately.
		head.GroupMode = MaxMode(mode, head.GroupMode)
		req.Status = LockGranted
		head.mu.Unlock()
		return nil
	}

	// Wait for the lock.
	head.Waiting = true
	req.Status = LockWaiting
	head.mu.Unlock()

	head.Cond.L.Lock()
	for req.Status == LockWaiting {
		head.Cond.Wait()
	}
	head.Cond.L.Unlock()

	return nil
}

func (lm *LockManager) Unlock(lockid uint64, obj *Object) {
	lm.mu.Lock()
	head, ok := lm.locks[*obj]
	if !ok {
		lm.mu.Unlock()
		return
	}
	head.mu.Lock()

	// get the lock owned by this transaction
	req := head.PerID[lockid]

	// if there is no lock owned by this transaction on the given object, return
	if req == nil {
		head.mu.Unlock()
		lm.mu.Unlock()
		return
	}

	// if this is the only request in the queue, remove the request
	// and the queue header.
	if head.Queue == req && req.Next == nil {
		head.mu.Unlock()
		delete(lm.locks, *obj)
		head.Reset()
		headPool.Put(head)
		lm.mu.Unlock()

		req.Reset()
		reqPool.Put(req)
		return
	}

	// remove the request from the queue
	if req.Prev != nil {
		req.Prev.Next = req.Next
	}
	if req.Next != nil {
		req.Next.Prev = req.Prev
	}
	if head.Queue == req {
		head.Queue = req.Next
	}

	// if this is the last request in the queue, update the last pointer
	if req.Next == nil {
		head.Last = req.Prev
	}

	head.Waiting = false
	head.GroupMode = Free

	delete(head.PerID, lockid)
	req.Reset()
	reqPool.Put(req)

	// wake up all compatible requests
	for req = head.Queue; req != nil; req = req.Next {
		// refresh the group mode with granted requests
		if req.Status == LockGranted {
			head.GroupMode = MaxMode(req.Mode, head.GroupMode)
			continue
		}

		// deal with waiting requests
		// if the lock is compatible with the current mode, grant it
		if head.GroupMode.IsCompatibleWith(req.Mode) {
			req.Status = LockGranted
			head.GroupMode = MaxMode(req.Mode, head.GroupMode)
		} else {
			// stop here
			head.Waiting = true
			break
		}
	}

	head.Cond.L.Lock()
	head.Cond.Broadcast()
	head.Cond.L.Unlock()

	head.mu.Unlock()
	lm.mu.Unlock()
}
