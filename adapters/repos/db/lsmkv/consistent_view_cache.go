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

package lsmkv

import (
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/errors"
)

type ConsistentViewCache interface {
	Get() BucketConsistentView
	Invalidate()
}

// ----------------------------------------------------------------------------

type ConsistentViewCacheNoop struct {
	createConsistentView func() BucketConsistentView
}

func NewConsistentViewCacheNoop(createConsistentView func() BucketConsistentView) *ConsistentViewCacheNoop {
	return &ConsistentViewCacheNoop{createConsistentView: createConsistentView}
}

func (c *ConsistentViewCacheNoop) Get() BucketConsistentView {
	return c.createConsistentView()
}

func (c *ConsistentViewCacheNoop) Invalidate() {}

// ----------------------------------------------------------------------------

type ConsistentViewCacheDefault struct {
	cached  *refsView
	lock    *sync.RWMutex
	invalid *atomic.Bool

	logger               logrus.FieldLogger
	createConsistentView func() BucketConsistentView
}

// Creates cache service to provide shared consistent view as long as it stays valid.
// Invalidation should be called on
// - memtables switch (new active is created, old active becomes flushing)
// - memtable flush (flushing memtable is removed, new segment is created)
// - compaction or cleanup (new segment replaces old segment(s))
//
// Usage of consistent view is tracked by references counter, incremented with every Get call
// and decremented on BucketConsistentView::ReleaseView call.
// Underlying consistent view is released when its reference counter is 0 and view is invalidated
// (can no longer be served by cache)
func NewConsistentViewCache(logger logrus.FieldLogger, createConsistentView func() BucketConsistentView) *ConsistentViewCacheDefault {
	return &ConsistentViewCacheDefault{
		lock:                 new(sync.RWMutex),
		invalid:              new(atomic.Bool),
		logger:               logger,
		createConsistentView: createConsistentView,
	}
}

// Gets consistent view from cache. If no view is cached, new one will be created.
// If cached was invalidated, but not yet cleared, new consistent view will replace previous one.
func (c *ConsistentViewCacheDefault) Get() BucketConsistentView {
	// no invalidation needed
	if !c.invalid.Load() {
		// return view if already exists
		c.lock.RLock()
		if refsView := c.cached; refsView != nil {
			defer c.lock.RUnlock()
			refsView.refsCounter.Add(1)
			return *refsView.view
		}
		c.lock.RUnlock()

		// return view if already exists (created by other call) or create new one
		c.lock.Lock()
		defer c.lock.Unlock()
		if c.cached == nil {
			c.cached = c.newRefsView()
		}
		c.cached.refsCounter.Add(1)
		return *c.cached.view
	}

	// invalidation needed
	var prevRefsCount int32
	var prevRefsView *refsView
	var view BucketConsistentView

	func() {
		c.lock.Lock()
		defer c.lock.Unlock()

		// check if invalidation not yet handled. clear cache if so
		if c.invalid.CompareAndSwap(true, false) {
			if prevRefsView = c.cached; prevRefsView != nil {
				prevRefsCount = prevRefsView.refsCounter.Load()
			}
			c.cached = nil
		}
		if c.cached == nil {
			c.cached = c.newRefsView()
		}
		c.cached.refsCounter.Add(1)
		view = *c.cached.view
	}()

	// release previous underlying consistent view if no references
	if prevRefsCount == 0 && prevRefsView != nil {
		errors.GoWrapper(prevRefsView.viewRelease, c.logger)
	}
	return view
}

// Invalidates consistent view in cache (if created before)
// Does not create new consistent view in replace.
//
// Initially it just marks cache as invalid, so that method is fast and do not use locks to avoid
// lock contention / deadlock when called inside bucket's flushlock or segmentgroup's maintenance lock.
// Actual cache invalidation is executed by goroutine or Get call, whichever happens first.
// Underlying consistent view will be released when reference counter drops to 0.
func (c *ConsistentViewCacheDefault) Invalidate() {
	c.invalid.Store(true)

	errors.GoWrapper(func() {
		if !c.invalid.Load() {
			// nothing to do, already processed
			return
		}

		var prevRefsCount int32
		var prevRefsView *refsView

		c.lock.Lock()
		// check if invalidation not yet handled. clear cache if so
		if c.invalid.CompareAndSwap(true, false) {
			if prevRefsView = c.cached; prevRefsView != nil {
				prevRefsCount = prevRefsView.refsCounter.Load()
				c.cached = nil
			}
		}
		c.lock.Unlock()

		// release previous underlying consistent view if no references
		if prevRefsCount == 0 && prevRefsView != nil {
			prevRefsView.viewRelease()
		}
	}, c.logger)
}

func (c *ConsistentViewCacheDefault) newRefsView() *refsView {
	consistentView := c.createConsistentView()
	refsView := &refsView{
		view:        &consistentView,
		viewRelease: consistentView.release,
	}

	refsView.view.release = func() {
		c.lock.RLock()
		refsCount := refsView.refsCounter.Add(-1)
		isCached := refsView == c.cached
		c.lock.RUnlock()

		// release underlying consistent view if no references and it is no longer cached/valid
		if refsCount == 0 && !isCached {
			refsView.viewRelease()
		}
	}
	return refsView
}

// ----------------------------------------------------------------------------

type refsView struct {
	view        *BucketConsistentView
	viewRelease func()
	refsCounter atomic.Int32
}
