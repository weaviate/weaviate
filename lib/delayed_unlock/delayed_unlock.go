/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
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
