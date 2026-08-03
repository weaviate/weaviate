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

// allowListDedupe coalesces concurrent legs of one query into one filter
// allow-list build per shard, keyed by the dedupe token. Entries live only
// while their build is in flight, so a stale bitmap is never handed out.
//
// Lock order: allowListDedupe.mu before allowListBuild.mu; publish drops the
// build lock before touching the map.
type allowListDedupe struct {
	mu       sync.Mutex
	inFlight map[string]*allowListBuild
}

// allowListBuild is an in-flight build with reference-counted ownership:
// every joiner owes one drop, and the buffer returns to the pool only when
// the count reaches zero.
type allowListBuild struct {
	done   chan struct{}
	filter *filters.LocalFilter

	// mu guards refs/waiters/owner/bm together: the release decision needs the
	// count and the owner as one value, and returning a pooled buffer twice
	// aliases it into two later queries.
	mu   sync.Mutex
	refs int
	// waiters counts participants that intend to consume the result; refs alone
	// can't express that because join holds a reference across the filter check.
	waiters int
	// published closes the group to new participants. Without it a joiner can
	// still find the entry after publish has counted the waiters, admit too
	// late, and then report a share the leader never counted.
	published bool
	owner     helpers.AllowList
	bm        *sroar.Bitmap
}

// do coalesces callers sharing a token and filter into one build; any failure
// falls back to an independent build rather than propagating another caller's
// outcome.
//
// The second return is the dedupe outcome, "" when never a candidate. do
// leaves recording it to the caller, so no outcome is counted twice.
func (d *allowListDedupe) do(ctx context.Context, token string, filter *filters.LocalFilter,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, string, error) {
	if token == "" || filter == nil {
		list, err := build(ctx)
		return list, "", err
	}

	b, leader, fallback := d.join(token, filter)
	if b == nil {
		list, err := build(ctx)
		return list, fallback, err
	}

	if leader {
		return d.lead(ctx, token, b, build)
	}

	select {
	case <-b.done:
	case <-ctx.Done():
		b.leave()
		return nil, helpers.AllowListDedupeCancelled, ctx.Err()
	}

	owner, bm := b.result()
	if owner == nil {
		b.leave()
		list, err := build(ctx)
		return list, helpers.AllowListDedupeLeaderFailed, err
	}

	return b.handle(bm), helpers.AllowListDedupeShared, nil
}

// lead runs the build for a group and always publishes an outcome, so waiters
// are never left blocked on an entry whose leader has gone. No path here may
// drop before publishing, or the refcount could reach zero while the entry is
// still reachable via d.inFlight.
func (d *allowListDedupe) lead(ctx context.Context, token string, b *allowListBuild,
	build func(context.Context) (helpers.AllowList, error),
) (helpers.AllowList, string, error) {
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
		return list, helpers.AllowListDedupeUnshared, err
	}

	shared := d.publish(token, b, list, shareable.Bm)
	published = true

	outcome := helpers.AllowListDedupeUnshared
	if shared {
		outcome = helpers.AllowListDedupeShared
	}
	return b.handle(shareable.Bm), outcome, nil
}

// join registers a participant for token, returning the entry to wait on and
// whether the caller must lead the build. A nil entry means build without
// dedupe, and the third return is the outcome that caller must report.
//
// The filter comparison runs outside d.mu: a retained reference keeps the
// entry alive while comparing, without holding the shard-wide lock for it.
func (d *allowListDedupe) join(token string, filter *filters.LocalFilter) (*allowListBuild, bool, string) {
	d.mu.Lock()
	existing, ok := d.inFlight[token]
	if !ok {
		b := &allowListBuild{done: make(chan struct{}), filter: filter, refs: 1}
		if d.inFlight == nil {
			d.inFlight = make(map[string]*allowListBuild, 1)
		}
		d.inFlight[token] = b
		d.mu.Unlock()
		return b, true, ""
	}
	existing.retain()
	d.mu.Unlock()

	// Verify rather than assume: sharing across a filter mismatch would silently
	// return wrong results.
	if !sameFilter(existing.filter, filter) {
		existing.drop()
		return nil, false, helpers.AllowListDedupeFilterMismatch
	}

	if !existing.admit() {
		// The build finished while this caller was joining, so there is nothing
		// in flight to share and the leader has already counted its waiters.
		existing.drop()
		return nil, false, helpers.AllowListDedupeUnshared
	}
	return existing, false, ""
}

// publish hands the build's outcome to the waiters, stops new joiners, and
// reports whether anyone was waiting — the shared/unshared distinction.
func (d *allowListDedupe) publish(token string, b *allowListBuild,
	owner helpers.AllowList, bm *sroar.Bitmap,
) bool {
	b.mu.Lock()
	b.owner, b.bm = owner, bm
	// Closing the group and counting the waiters must be one step, or a joiner
	// admitted between them shares a bitmap the leader reported as unshared.
	b.published = true
	shared := b.waiters > 0
	b.mu.Unlock()

	d.mu.Lock()
	if d.inFlight[token] == b {
		delete(d.inFlight, token)
	}
	d.mu.Unlock()

	close(b.done)
	return shared
}

// retain adds one reference without claiming the result, so a caller can read
// the entry outside d.mu.
func (b *allowListBuild) retain() {
	b.mu.Lock()
	b.refs++
	b.mu.Unlock()
}

// admit promotes a retained reference into a participant that intends to consume
// the result. It reports false once the build has published, when joining would
// mean taking a bitmap from a build that had already finished.
func (b *allowListBuild) admit() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.published {
		return false
	}
	b.waiters++
	return true
}

// leave releases an admitted participant that ends up not consuming the
// result — the counterpart to admit, so giving up early can't look shared.
func (b *allowListBuild) leave() {
	b.mu.Lock()
	b.waiters--
	b.mu.Unlock()
	b.drop()
}

func (b *allowListBuild) drop() {
	b.mu.Lock()
	b.refs--
	if b.refs < 0 {
		b.mu.Unlock()
		// Unreachable by construction: every participant owes exactly one drop,
		// and lead publishes before dropping. An extra release means a bug
		// elsewhere; fail here rather than leave refs negative.
		panic("allowListBuild: negative reference count")
	}
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

// handle converts this participant's reference into an AllowList. It returns
// a concrete *helpers.BitmapAllowList, not a wrapper: the block-max WAND path
// type-asserts to that type, and a wrapper would silently break it.
func (b *allowListBuild) handle(bm *sroar.Bitmap) helpers.AllowList {
	var once sync.Once
	return helpers.NewAllowListCloseableFromBitmap(bm, func() { once.Do(b.drop) })
}

// sameFilter reports whether two filter trees resolve to the same doc IDs. It
// errs towards false, which only costs the dedupe, never correctness.
//
// Both legs reach a shard the same way: one shared pointer locally, two
// independent deserializations remotely. Two deserializations do compare equal,
// measured on a 4-node rig. Mixing a local tree with a wire one would not (a
// Date stays a string), but that pair is unreachable, because legs on different
// replicas land on different nodes and never meet.
func sameFilter(a, b *filters.LocalFilter) bool {
	if a == b {
		return true
	}
	return reflect.DeepEqual(a, b)
}
