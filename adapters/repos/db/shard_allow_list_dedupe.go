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
//
// Lock order is allowListDedupe.mu before allowListBuild.mu. Nothing takes them
// the other way round: publish releases the build lock before it touches the
// map.
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

	// mu guards the entire ownership state below. The release decision needs the
	// refcount and the owner together, and returning a pooled buffer twice
	// aliases one buffer into two later queries, so that pair is read under a
	// lock rather than left to the evaluation order of an `&&`.
	mu    sync.Mutex
	refs  int
	owner helpers.AllowList
	bm    *sroar.Bitmap
}

// do returns a filter allow list, coalescing callers that share a token and an
// equal filter into one build. Every failure mode (error, cancellation) falls
// back to an independent build rather than propagating another caller's outcome.
//
// The second return value is the dedupe outcome, empty when the call was never
// a dedupe candidate.
func (d *allowListDedupe) do(ctx context.Context, token string, filter *filters.LocalFilter,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, string, error) {
	list, outcome, err := d.run(ctx, token, filter, build)
	if outcome != "" {
		helpers.RecordAllowListDedupe(outcome)
	}
	return list, outcome, err
}

func (d *allowListDedupe) run(ctx context.Context, token string, filter *filters.LocalFilter,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, string, error) {
	if token == "" || filter == nil {
		list, err := build(ctx)
		return list, "", err
	}

	b, leader := d.join(token, filter)
	if b == nil {
		list, err := build(ctx)
		return list, helpers.AllowListDedupeFilterMismatch, err
	}

	if leader {
		list, err := d.lead(ctx, token, b, build)
		return list, helpers.AllowListDedupeUnshared, err
	}

	select {
	case <-b.done:
	case <-ctx.Done():
		b.drop()
		return nil, helpers.AllowListDedupeCancelled, ctx.Err()
	}

	owner, bm := b.result()
	if owner == nil {
		b.drop()
		list, err := build(ctx)
		return list, helpers.AllowListDedupeLeaderFailed, err
	}

	return b.handle(bm), helpers.AllowListDedupeShared, nil
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
	return b.handle(shareable.Bm), nil
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
		existing.retain()
		return existing, false
	}

	b := &allowListBuild{done: make(chan struct{}), filter: filter, refs: 1}
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
	b.mu.Lock()
	b.owner, b.bm = owner, bm
	b.mu.Unlock()

	d.mu.Lock()
	if d.inFlight[token] == b {
		delete(d.inFlight, token)
	}
	d.mu.Unlock()

	close(b.done)
}

// retain adds one reference. Callers hold allowListDedupe.mu so that joining an
// entry and counting the join cannot straddle its publish.
func (b *allowListBuild) retain() {
	b.mu.Lock()
	b.refs++
	b.mu.Unlock()
}

func (b *allowListBuild) drop() {
	b.mu.Lock()
	b.refs--
	last, owner := b.refs == 0, b.owner
	b.mu.Unlock()

	if last && owner != nil {
		owner.Close()
	}
}

// result reports what the build published. A nil owner means it produced
// nothing shareable.
func (b *allowListBuild) result() (helpers.AllowList, *sroar.Bitmap) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.owner, b.bm
}

// handle converts this participant's reference into an AllowList of its own.
// It returns a *helpers.BitmapAllowList, not a wrapper: the block-max WAND path
// type-asserts to that concrete type, and a wrapper would silently break it.
func (b *allowListBuild) handle(bm *sroar.Bitmap) helpers.AllowList {
	var once sync.Once
	return helpers.NewAllowListCloseableFromBitmap(bm, func() { once.Do(b.drop) })
}

// sameFilter reports whether two filter trees resolve to the same doc IDs. It
// errs towards false, which only costs the dedupe, never correctness.
func sameFilter(a, b *filters.LocalFilter) bool {
	if a == b {
		return true
	}
	return reflect.DeepEqual(a, b)
}
