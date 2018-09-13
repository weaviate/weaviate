package delayed_unlock

import "sync"

type Unlockable interface {
	Unlock()
}

// Delay unlocking some interface for a number of steps.
type DelayedUnlockable struct {
	sync.Mutex
	toUnlock Unlockable
	steps    int
}

func New(u Unlockable) *DelayedUnlockable {
	return &DelayedUnlockable{
		toUnlock: u,
		steps:    1,
	}
}

func (d *DelayedUnlockable) IncSteps() {
	if d.steps > 0 {
		d.steps += 1
	} else {
		panic("Already released")
	}
}

func (d *DelayedUnlockable) Go(f func()) {
	d.IncSteps()

	go func() {
		defer d.Unlock()
		f()
	}()
}

func (d *DelayedUnlockable) Unlock() {
	d.Lock()
	defer d.Unlock()

	d.steps -= 1

	if d.steps == 0 {
		d.toUnlock.Unlock()
	} else if d.steps < 0 {
		panic("Already unlocked")
	}
}
