// Package lock implements two-phase locking (2PL).
// It supports explicit and intent locks.
package lockmanager

import "sync"

// LockMode represents the type of lock a transaction
// wants to acquire on a database object.
type LockMode int

const (
	// Free represents no lock.
	Free LockMode = iota
	// Intent shared.
	IS
	// Intent exclusive.
	IX
	// Shared.
	S
	// Shared intent exclusive.
	SIX
	// Exclusive.
	X
)

// IsComptibleWith returns true if the lock mode can be held at the same
// time as the other lock mode.
func (l LockMode) IsCompatibleWith(other LockMode) bool {
	switch l {
	case Free:
		return true
	case IS:
		return other == IS || other == IX || other == S || other == SIX
	case IX:
		return other == IS || other == IX
	case S:
		return other == IS || other == S
	case SIX:
		return other == SIX
	case X:
		return false
	}

	return false
}

// MaxMode returns the maximum lock mode between two lock modes.
func MaxMode(requested, granted LockMode) LockMode {
	switch requested {
	case IS:
		switch granted {
		case Free:
			return IS
		case IS:
			return IS
		case IX:
			return IX
		case S:
			return S
		case SIX:
			return SIX
		case X:
			return X
		}
	case IX:
		switch granted {
		case Free:
			return IX
		case IS:
			return IX
		case IX:
			return IX
		case S:
			return SIX
		case SIX:
			return SIX
		case X:
			return X
		}
	case S:
		switch granted {
		case Free:
			return S
		case IS:
			return S
		case IX:
			return SIX
		case S:
			return S
		case SIX:
			return SIX
		case X:
			return X
		}
	case SIX:
		switch granted {
		case Free:
			return SIX
		case IS:
			return SIX
		case IX:
			return SIX
		case S:
			return SIX
		case SIX:
			return SIX
		case X:
			return X
		}
	case X:
		return X
	}

	panic("unreachable")
}

type LockStatus int

const (
	LockGranted LockStatus = iota
	LockConverting
	LockWaiting
	LockDenied
)

// A LockHeader holds the current state of a lock for a
// given object. It also points to the lock request queue.
type LockHeader struct {
	mu sync.Mutex

	Object    *Object      // The object this lock is for.
	Queue     *LockRequest // The queue of pending lock requests
	Last      *LockRequest // The last request in the queue
	GroupMode LockMode     // The current mode of the group
	Waiting   bool         // Indicates if there are pending lock requests
}

// A LockRequest holds the information about a lock request
// make by a transaction.
type LockRequest struct {
	Next        *LockRequest  // Next request in the queue
	Head        *LockHeader   // Head of the queue
	Status      LockStatus    // Status of the request
	Mode        LockMode      // Mode requested / granted
	ConvertMode LockMode      // Mode to convert to
	WakeUp      chan struct{} // Channel to wake up the transaction
	Count       int           // Number of times this lock was requested
	Lockid      uint64        // ID of the lock request
}

func (lr *LockRequest) Reset() {
	lr.Next = nil
	lr.Head = nil
	lr.Status = LockGranted
	lr.Mode = Free
	lr.ConvertMode = Free
	lr.WakeUp = nil
	lr.Count = 0
	lr.Lockid = 0
}

// An Object to be locked. It supports multiple
// hierarchical levels.
// Ex: Locking the full database can be level 0,
// locking a shard can be level 1, etc.
type Object struct {
	// The level of this object in the hierarchy of locked objects.
	Level int
	// The unique ID of this object.
	ID uint64
}
