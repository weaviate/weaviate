package delayed_unlock

import "sync"

type Unlockable interface {
	Unlock()
}

type DelayedUnlockable interface {
	Unlockable

	IncSteps()
	Go(f func())
}

// Delay unlocking some interface for a number of steps.
type delayedUnlockable struct {
	inner    sync.Mutex
	toUnlock Unlockable
	steps    int
}

func New(u Unlockable) DelayedUnlockable {
	return &delayedUnlockable{
		toUnlock: u,
		steps:    1,
	}
}

func (d *delayedUnlockable) IncSteps() {
	d.inner.Lock()
	defer d.inner.Unlock()

	if d.steps > 0 {
		d.steps += 1
	} else {
		panic("Already released")
	}
}

func (d *delayedUnlockable) Go(f func()) {
	d.IncSteps()

	go func() {
		defer d.Unlock()
		f()
	}()
}

func (d *delayedUnlockable) Unlock() {
	d.inner.Lock()
	defer d.inner.Unlock()

	d.steps -= 1

	if d.steps == 0 {
		d.toUnlock.Unlock()
	} else if d.steps < 0 {
		panic("Already unlocked")
	}
}
