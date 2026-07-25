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

package db

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/filters"
)

// allowListDedupe lets several legs of one query share one filter allow-list
// build per shard, keyed by the query's dedupe token.
//
// Coalescing is strictly in-flight: an entry is removed the moment its build
// publishes, so a leg can never be handed an already-finished, stale bitmap.
// The zero value is ready to use.
type allowListDedupe struct {
	mu       sync.Mutex
	inFlight map[string]*allowListBuild
}

// allowListBuild is one in-flight build with reference-counted ownership of its
// result: every joiner owes exactly one drop, and the bitmap buffer returns to
// the pool only on the drop that takes the count to zero.
type allowListBuild struct {
	done   chan struct{}
	filter *filters.LocalFilter

	// refs is taken under allowListDedupe.mu; the leader holds the first
	// reference until after it publishes, so refs cannot reach zero before
	// owner is written.
	refs atomic.Int32

	// owner and bm are written by the leader before done is closed, so any
	// reader that observes done (or a subsequent zero refs count) sees them.
	owner helpers.AllowList
	bm    *sroar.Bitmap
}

// do returns a filter allow list, coalescing callers that share a token and an
// equal filter into one build. Every failure mode (error, cancellation) falls
// back to an independent build rather than propagating another caller's outcome.
func (d *allowListDedupe) do(ctx context.Context, token string, filter *filters.LocalFilter,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, bool, error) {
	if token == "" || filter == nil {
		list, err := build(ctx)
		return list, false, err
	}

	b, leader := d.join(token, filter)
	if b == nil {
		list, err := build(ctx)
		return list, false, err
	}

	if leader {
		list, err := d.lead(ctx, token, b, build)
		return list, false, err
	}

	select {
	case <-b.done:
	case <-ctx.Done():
		b.drop()
		return nil, false, ctx.Err()
	}

	if b.owner == nil {
		b.drop()
		list, err := build(ctx)
		return list, false, err
	}

	return b.handle(), true, nil
}

// lead runs the build for a group and always publishes an outcome, so waiters
// are never left blocked on an entry whose leader has gone.
func (d *allowListDedupe) lead(ctx context.Context, token string, b *allowListBuild,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, error) {
	published := false
	defer func() {
		if !published {
			// A panicking build must still release waiters, or they block until
			// their own context expires.
			d.publish(token, b, nil, nil)
			b.drop()
		}
	}()

	list, err := build(ctx)

	// Only the bitmap-backed list has an ownership model we can split across
	// legs; anything else stays private to this caller.
	shareable, _ := list.(*helpers.BitmapAllowList)
	if err != nil || shareable == nil {
		d.publish(token, b, nil, nil)
		published = true
		b.drop()
		return list, err
	}

	d.publish(token, b, list, shareable.Bm)
	published = true
	return b.handle(), nil
}

// join registers a participant for token, returning the entry to wait on and
// whether the caller must lead the build. A nil entry means: build without
// dedupe.
func (d *allowListDedupe) join(token string, filter *filters.LocalFilter) (*allowListBuild, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if existing, ok := d.inFlight[token]; ok {
		// Verify rather than assume: sharing across a filter mismatch would
		// silently return wrong results.
		if !sameFilter(existing.filter, filter) {
			return nil, false
		}
		existing.refs.Add(1)
		return existing, false
	}

	b := &allowListBuild{done: make(chan struct{}), filter: filter}
	b.refs.Store(1)
	if d.inFlight == nil {
		d.inFlight = make(map[string]*allowListBuild, 1)
	}
	d.inFlight[token] = b
	return b, true
}

// publish hands the build's outcome to the waiters and stops new participants
// from joining it.
func (d *allowListDedupe) publish(token string, b *allowListBuild,
	owner helpers.AllowList, bm *sroar.Bitmap,
) {
	b.owner, b.bm = owner, bm

	d.mu.Lock()
	if d.inFlight[token] == b {
		delete(d.inFlight, token)
	}
	d.mu.Unlock()

	close(b.done)
}

func (b *allowListBuild) drop() {
	if b.refs.Add(-1) == 0 && b.owner != nil {
		b.owner.Close()
	}
}

// handle converts this participant's reference into an AllowList of its own.
// It returns a *helpers.BitmapAllowList, not a wrapper: the block-max WAND path
// type-asserts to that concrete type, and a wrapper would silently break it.
func (b *allowListBuild) handle() helpers.AllowList {
	var once sync.Once
	return helpers.NewAllowListCloseableFromBitmap(b.bm, func() { once.Do(b.drop) })
}

// sameFilter reports whether two filter trees resolve to the same doc IDs. It
// errs towards false, which only costs the dedupe, never correctness.
func sameFilter(a, b *filters.LocalFilter) bool {
	if a == b {
		return true
	}
	return reflect.DeepEqual(a, b)
}
