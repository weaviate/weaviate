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
	"maps"
	"slices"

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

// migrationScopeReg is one registration's own claim on the scope, under the id
// its callback pair carries. Two migrations mirroring one property is a steady
// state while a failed generation waits to be superseded, so the scope has to
// record who claims what: union-and-subtract over one shared set lets either
// migration's disarm strip a property the other still mirrors, which
// un-suppresses the inline write path into the survivor's staged bucket.
type migrationScopeReg struct {
	id      uint64
	props   map[string]struct{}
	overlay map[string]inverted.PropertyOverlay
}

// deriveScope rebuilds the write path's scope from whichever registrations
// survive, so the scope cannot disagree with the callbacks. Regs stay in
// ascending id order, so a property two of them overlay differently takes the
// most recent arm's value; those properties are returned for the caller to
// report. It returns nil maps when nothing is armed, which is the write path's
// idle fast path.
func deriveScope(regs []migrationScopeReg) (migrationDoubleWriteScope, []string) {
	var (
		next      migrationDoubleWriteScope
		conflicts []string
	)
	for _, reg := range regs {
		for prop := range reg.props {
			if next.props == nil {
				next.props = make(map[string]struct{}, len(reg.props))
			}
			next.props[prop] = struct{}{}
		}
		for prop, overlay := range reg.overlay {
			// Once per property, not once per disagreeing pair: three
			// registrations that all differ are still one thing to report.
			if prev, ok := next.overlay[prop]; ok && prev != overlay && !slices.Contains(conflicts, prop) {
				conflicts = append(conflicts, prop)
			}
			if next.overlay == nil {
				next.overlay = make(map[string]inverted.PropertyOverlay, len(reg.overlay))
			}
			next.overlay[prop] = overlay
		}
	}
	return next, conflicts
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
// callbacks-without-scope or scope-without-callbacks to a write. scope is
// derived from scopeRegs on every mutation rather than accumulated, which is
// what extends that guarantee across two registrations on one property.
//
// nextCallbackID hands out per-registration ids under mutatePropValueIndexState's
// mutex; it is carried by copy across mutations so every registration gets a
// distinct id its disarm can remove by.
type propValueIndexState struct {
	add            []addCallbackEntry
	del            []deleteCallbackEntry
	scope          migrationDoubleWriteScope
	scopeRegs      []migrationScopeReg
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

// replaceAddCallback returns a fresh slice with the entry carrying id swapped
// for cb. Replacing rather than removing is what makes one registration's
// properties separable: a disarm that drops one property re-registers the pair
// over the properties that are left, so the write path keeps carrying one
// callback per migration instead of one per property.
func replaceAddCallback(cur []addCallbackEntry, id uint64, cb onAddToPropertyValueIndex) []addCallbackEntry {
	updated := make([]addCallbackEntry, len(cur))
	copy(updated, cur)
	for i := range updated {
		if updated[i].id == id {
			updated[i].fn = cb
			break
		}
	}
	return updated
}

func replaceDeleteCallback(cur []deleteCallbackEntry, id uint64, cb onDeleteFromPropertyValueIndex) []deleteCallbackEntry {
	updated := make([]deleteCallbackEntry, len(cur))
	copy(updated, cur)
	for i := range updated {
		if updated[i].id == id {
			updated[i].fn = cb
			break
		}
	}
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
// (weaviate/0-weaviate-issues#298). The returned func disarms one property of
// this registration and no other registration's claim on it.
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
//     that torn state unobservable, so the flag is redundant. An in-flight
//     writer still holding the pre-disarm snapshot lands in this record's own
//     bucket under either name, because resolveScopedDoubleWriteBucket's
//     canonical fallback denotes the same physical bucket once this record
//     flipped. A straggler outliving ANOTHER record's flip is what the
//     retirement ordering handles, not this.
//
// Disarming a subset re-registers the pair over the properties that are left
// rather than removing it, because the actor that disarms owns one property of
// the scope — a successor's retirement takes over the properties it overlaps
// and no others. Rebuilding the callbacks is what keeps the write path
// carrying one pair per migration rather than one per property: every
// registered callback fires for every analyzed property, so a pair per
// property would cost the square of the migration's property count on every
// write.
//
// makeCallbacks receives the properties still armed and must build a pair
// scoped to exactly them.
func (s *Shard) registerDoubleWriteWithScope(props []string, overlay map[string]inverted.PropertyOverlay,
	makeCallbacks func(scope map[string]struct{}) (onAddToPropertyValueIndex, onDeleteFromPropertyValueIndex),
) func(disarming string) {
	armed := make(map[string]struct{}, len(props))
	for _, prop := range props {
		armed[prop] = struct{}{}
	}

	var id uint64
	add, del := makeCallbacks(maps.Clone(armed))
	s.mutateScopeRegs(func(cur propValueIndexState) propValueIndexState {
		id = cur.nextCallbackID
		cur.nextCallbackID++
		cur.add = appendAddCallback(cur.add, id, add)
		cur.del = appendDeleteCallback(cur.del, id, del)
		cur.scopeRegs = append(slices.Clone(cur.scopeRegs), migrationScopeReg{
			id: id, props: armed, overlay: maps.Clone(overlay),
		})
		return cur
	})

	return func(disarming string) {
		s.mutateScopeRegs(func(cur propValueIndexState) propValueIndexState {
			idx := slices.IndexFunc(cur.scopeRegs, func(reg migrationScopeReg) bool { return reg.id == id })
			if idx == -1 {
				return cur
			}
			reg := cur.scopeRegs[idx]
			if _, ok := reg.props[disarming]; !ok {
				return cur
			}

			remaining := maps.Clone(reg.props)
			delete(remaining, disarming)
			if len(remaining) == 0 {
				cur.add = removeAddCallback(cur.add, id)
				cur.del = removeDeleteCallback(cur.del, id)
				cur.scopeRegs = slices.Delete(slices.Clone(cur.scopeRegs), idx, idx+1)
				return cur
			}

			newAdd, newDel := makeCallbacks(maps.Clone(remaining))
			cur.add = replaceAddCallback(cur.add, id, newAdd)
			cur.del = replaceDeleteCallback(cur.del, id, newDel)
			reg.props = remaining
			reg.overlay = maps.Clone(reg.overlay)
			delete(reg.overlay, disarming)
			cur.scopeRegs = slices.Clone(cur.scopeRegs)
			cur.scopeRegs[idx] = reg
			return cur
		})
	}
}

// mutateScopeRegs re-derives the scope from whatever registrations fn leaves
// behind, in the same atomic transition, so the two can never drift apart. The
// conflict report is logged after the mutex is released.
func (s *Shard) mutateScopeRegs(fn func(cur propValueIndexState) propValueIndexState) {
	var conflicts []string
	s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
		cur = fn(cur)
		cur.scope, conflicts = deriveScope(cur.scopeRegs)
		return cur
	})
	for _, prop := range conflicts {
		// One analysis runs per property, so the older mirror is served the
		// newer arm's overlay. Only a cross-family overlap loses a flag.
		s.index.logger.WithField("property", prop).Warn(
			"two migrations mirror this property with different analyzer overlays; the most recent arm wins")
	}
}
