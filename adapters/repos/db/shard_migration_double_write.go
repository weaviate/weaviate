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
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storobj"
)

// migrationDoubleWriteScope names properties needing TARGET-schema analysis
// via overlay, so an overlapping write mirrors the backfill. Nil maps mean
// idle — no migration in flight.
type migrationDoubleWriteScope struct {
	props   map[string]struct{}
	overlay map[string]inverted.PropertyOverlay
}

// withArmed copies rather than mutates, so a concurrent lock-free reader keeps
// seeing the old snapshot until Store publishes the new one.
func (sc migrationDoubleWriteScope) withArmed(props []string,
	overlay map[string]inverted.PropertyOverlay,
) migrationDoubleWriteScope {
	next := migrationDoubleWriteScope{
		props:   make(map[string]struct{}, len(sc.props)+len(props)),
		overlay: make(map[string]inverted.PropertyOverlay, len(sc.overlay)+len(overlay)),
	}
	for k := range sc.props {
		next.props[k] = struct{}{}
	}
	for _, p := range props {
		next.props[p] = struct{}{}
	}
	for k, v := range sc.overlay {
		next.overlay[k] = v
	}
	for k, v := range overlay {
		next.overlay[k] = v
	}
	return next
}

// withDisarmed removes props/overlay keys and collapses to nil maps when
// empty, so the write path's zero-scope fast path re-engages after migration.
func (sc migrationDoubleWriteScope) withDisarmed(props []string,
	overlay map[string]inverted.PropertyOverlay,
) migrationDoubleWriteScope {
	var next migrationDoubleWriteScope
	for k := range sc.props {
		next.props = insertUnlessIn(next.props, k, props)
	}
	for k, v := range sc.overlay {
		if _, drop := overlay[k]; drop {
			continue
		}
		if next.overlay == nil {
			next.overlay = make(map[string]inverted.PropertyOverlay, len(sc.overlay))
		}
		next.overlay[k] = v
	}
	return next
}

func insertUnlessIn(dst map[string]struct{}, key string, drop []string) map[string]struct{} {
	for _, d := range drop {
		if d == key {
			return dst
		}
	}
	if dst == nil {
		dst = map[string]struct{}{}
	}
	dst[key] = struct{}{}
	return dst
}

// addCallbackEntry pairs a registered add callback with the id its disarm func
// removes it by. Go forbids comparing func values, so an explicit id lets
// disarm drop exactly this registration from the copy-on-write slice — the
// callback is REMOVED, not just flagged, so the slice can never grow without
// bound across a long-lived shard's migration history.
type addCallbackEntry struct {
	id uint64
	fn onAddToPropertyValueIndex
}

// deleteCallbackEntry is addCallbackEntry's delete-side counterpart.
type deleteCallbackEntry struct {
	id uint64
	fn onDeleteFromPropertyValueIndex
}

// propValueIndexState folds the callback slices and migration scope into one
// atomic snapshot, so a concurrent arm/disarm can never expose
// callbacks-without-scope or scope-without-callbacks to a write.
//
// nextCallbackID hands out per-registration ids under mutatePropValueIndexState's
// mutex; it is carried by copy across mutations so every registration gets a
// distinct id its disarm can remove by.
type propValueIndexState struct {
	add            []addCallbackEntry
	del            []deleteCallbackEntry
	scope          migrationDoubleWriteScope
	nextCallbackID uint64
}

// emptyPropValueIndexState is returned by loadPropValueIndexState before any
// callback has ever been registered, so callers never nil-check the Load.
var emptyPropValueIndexState = &propValueIndexState{}

// loadPropValueIndexState returns the current snapshot without locking. Load
// once per object so suppression and the migration pass see the same
// {add,del,scope}.
func (s *Shard) loadPropValueIndexState() *propValueIndexState {
	if v := s.propValueIndexState.Load(); v != nil {
		return v.(*propValueIndexState)
	}
	return emptyPropValueIndexState
}

// mutatePropValueIndexState is the sole writer of the folded snapshot: fn runs
// under the mutex and the result publishes via one atomic Store, so
// registration/arm/disarm land as one indivisible transition. fn must copy,
// not mutate in place, any slice/map it grows.
func (s *Shard) mutatePropValueIndexState(fn func(cur propValueIndexState) propValueIndexState) {
	s.propertyValueIndexCallbacksMu.Lock()
	defer s.propertyValueIndexCallbacksMu.Unlock()

	var cur propValueIndexState
	if v := s.propValueIndexState.Load(); v != nil {
		cur = *(v.(*propValueIndexState))
	}
	next := fn(cur)
	s.propValueIndexState.Store(&next)
}

// appendAddCallback returns a fresh slice (copy-on-write) with cb appended
// under id, so a lock-free reader iterating the old slice is never mutated.
func appendAddCallback(cur []addCallbackEntry, id uint64, cb onAddToPropertyValueIndex) []addCallbackEntry {
	updated := make([]addCallbackEntry, len(cur)+1)
	copy(updated, cur)
	updated[len(cur)] = addCallbackEntry{id: id, fn: cb}
	return updated
}

func appendDeleteCallback(cur []deleteCallbackEntry, id uint64, cb onDeleteFromPropertyValueIndex) []deleteCallbackEntry {
	updated := make([]deleteCallbackEntry, len(cur)+1)
	copy(updated, cur)
	updated[len(cur)] = deleteCallbackEntry{id: id, fn: cb}
	return updated
}

// removeAddCallback returns a fresh slice (copy-on-write) with the entry
// carrying id dropped, so disarm shrinks the slice a lock-free reader may be
// iterating without mutating that reader's copy. Returns cur unchanged (same
// backing array) when id is absent, making a double-disarm a no-op.
func removeAddCallback(cur []addCallbackEntry, id uint64) []addCallbackEntry {
	idx := -1
	for i := range cur {
		if cur[i].id == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return cur
	}
	updated := make([]addCallbackEntry, 0, len(cur)-1)
	updated = append(updated, cur[:idx]...)
	updated = append(updated, cur[idx+1:]...)
	return updated
}

func removeDeleteCallback(cur []deleteCallbackEntry, id uint64) []deleteCallbackEntry {
	idx := -1
	for i := range cur {
		if cur[i].id == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return cur
	}
	updated := make([]deleteCallbackEntry, 0, len(cur)-1)
	updated = append(updated, cur[:idx]...)
	updated = append(updated, cur[idx+1:]...)
	return updated
}

// fireAddToPropertyValueIndex invokes every add callback, bypassing the
// inline write path's scope suppression (the migration pass needs it fired).
func (s *Shard) fireAddToPropertyValueIndex(st *propValueIndexState, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range st.add {
		ec.Add(cb.fn(s, docID, property))
	}
	return ec.ToError()
}

func (s *Shard) fireDeleteFromPropertyValueIndex(st *propValueIndexState, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range st.del {
		ec.Add(cb.fn(s, docID, property))
	}
	return ec.ToError()
}

// analyzeForDoubleWrite filters AnalyzeObjectForMigrationWithOverlay's result
// to scope properties, so the migration pass never touches a bucket it does
// not own.
func (s *Shard) analyzeForDoubleWrite(obj *storobj.Object, st *propValueIndexState) ([]inverted.Property, error) {
	props, _, err := s.AnalyzeObjectForMigrationWithOverlay(obj, st.scope.overlay)
	if err != nil {
		return nil, err
	}
	filtered := props[:0]
	for i := range props {
		if _, ok := st.scope.props[props[i].Name]; ok {
			filtered = append(filtered, props[i])
		}
	}
	return filtered, nil
}

// migrationDoubleWrite mirrors a write into the ingest bucket under TARGET
// analysis, for scope props whose inline callback was suppressed. The ingest
// bucket is a write-only sidecar until swap, so per-write churn is idempotent
// and invisible to queries.
func (s *Shard) migrationDoubleWrite(st *propValueIndexState, object, prevObject *storobj.Object,
	status objectInsertStatus,
) error {
	if len(st.scope.props) == 0 {
		return nil
	}

	if prevObject != nil {
		migDel, err := s.analyzeForDoubleWrite(prevObject, st)
		if err != nil {
			return err
		}
		for i := range migDel {
			if err := s.fireDeleteFromPropertyValueIndex(st, status.oldDocID, &migDel[i]); err != nil {
				return err
			}
		}
	}

	migAdd, err := s.analyzeForDoubleWrite(object, st)
	if err != nil {
		return err
	}
	for i := range migAdd {
		if err := s.fireAddToPropertyValueIndex(st, status.docID, &migAdd[i]); err != nil {
			return err
		}
	}
	return nil
}

// migrationDoubleWriteDelete is migrationDoubleWrite's delete-only
// counterpart for the pure object-delete path.
func (s *Shard) migrationDoubleWriteDelete(st *propValueIndexState, prevObject *storobj.Object, docID uint64) error {
	if len(st.scope.props) == 0 || prevObject == nil {
		return nil
	}
	migDel, err := s.analyzeForDoubleWrite(prevObject, st)
	if err != nil {
		return err
	}
	for i := range migDel {
		if err := s.fireDeleteFromPropertyValueIndex(st, docID, &migDel[i]); err != nil {
			return err
		}
	}
	return nil
}

// registerDoubleWriteWithScope arms the scope and registers the add+delete
// callbacks in ONE atomic Store, so a concurrent writer never sees callbacks
// without the scope and leaks source-tokenized terms into the ingest bucket
// (weaviate/0-weaviate-issues#298). Returned func disarms both.
//
// Disarm REMOVES the callbacks (by id) in the SAME atomic mutate that drops the
// scope. Two consequences:
//
//   - No unbounded growth. Earlier this only flagged the closures disabled and
//     left them in the slice, so every past migration's pair stayed on the hot
//     write path forever — O(migrations) per-write cost plus a slow leak on
//     long-lived shards. Removing them keeps the slice bounded by the number of
//     migrations in flight.
//   - No disabled-flag guard needed. A flag was only ever required because the
//     old disarm dropped the scope while leaving the callbacks present,
//     transiently exposing a {scope-absent, callback-present} state a writer
//     would double-write through. Removing callback and scope together makes
//     that torn state unobservable, so the flag is redundant. Any in-flight
//     writer still holding the pre-disarm snapshot fires the callback safely:
//     resolveScopedDoubleWriteBucket lands the mirror in the surviving canonical
//     bucket (target phase) or no-ops when the sidecar is gone (backup phase).
func (s *Shard) registerDoubleWriteWithScope(add onAddToPropertyValueIndex, del onDeleteFromPropertyValueIndex,
	props []string, overlay map[string]inverted.PropertyOverlay,
) func() {
	var id uint64
	s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
		id = cur.nextCallbackID
		cur.nextCallbackID++
		cur.add = appendAddCallback(cur.add, id, add)
		cur.del = appendDeleteCallback(cur.del, id, del)
		cur.scope = cur.scope.withArmed(props, overlay)
		return cur
	})

	return func() {
		s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
			cur.add = removeAddCallback(cur.add, id)
			cur.del = removeDeleteCallback(cur.del, id)
			cur.scope = cur.scope.withDisarmed(props, overlay)
			return cur
		})
	}
}
