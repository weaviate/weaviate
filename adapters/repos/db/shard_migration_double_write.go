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

type migrationScopeReg struct {
	id      uint64
	props   map[string]struct{}
	overlay map[string]inverted.PropertyOverlay
}

func deriveScope(regs []migrationScopeReg) (migrationDoubleWriteScope, []string) {
	var (
		next      migrationDoubleWriteScope
		conflicts []string
	)
	wanted := map[string]inverted.PropertyOverlay{}
	for _, reg := range regs {
		for prop := range reg.props {
			if next.props == nil {
				next.props = make(map[string]struct{}, len(reg.props))
			}
			next.props[prop] = struct{}{}

			overlay := reg.overlay[prop]
			if prev, ok := wanted[prop]; ok && prev != overlay && !slices.Contains(conflicts, prop) {
				conflicts = append(conflicts, prop)
			}
			wanted[prop] = overlay
		}
		for prop, overlay := range reg.overlay {
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
	add             []addCallbackEntry
	del             []deleteCallbackEntry
	scope           migrationDoubleWriteScope
	scopeRegs       []migrationScopeReg
	nextCallbackID  uint64
	overlaysDiverge bool
	conflicts       []string
	analyses        []doubleWriteAnalysis
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
		cur = *v.(*propValueIndexState)
	}
	next := fn(cur)
	next.analyses = next.buildDoubleWriteAnalyses()
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

func (s *Shard) fireAddToPropertyValueIndex(callbacks []addCallbackEntry, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range callbacks {
		ec.Add(cb.fn(s, docID, property))
	}
	return ec.ToError()
}

func (s *Shard) fireDeleteFromPropertyValueIndex(callbacks []deleteCallbackEntry, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range callbacks {
		ec.Add(cb.fn(s, docID, property))
	}
	return ec.ToError()
}

type doubleWriteAnalysis struct {
	props   map[string]struct{}
	overlay map[string]inverted.PropertyOverlay
	add     []addCallbackEntry
	del     []deleteCallbackEntry
}

func (st *propValueIndexState) buildDoubleWriteAnalyses() []doubleWriteAnalysis {
	if len(st.scope.props) == 0 {
		return nil
	}
	if !st.overlaysDiverge {
		return []doubleWriteAnalysis{{
			props:   st.scope.props,
			overlay: st.scope.overlay,
			add:     st.add,
			del:     st.del,
		}}
	}
	out := make([]doubleWriteAnalysis, 0, len(st.scopeRegs))
	for _, reg := range st.scopeRegs {
		out = append(out, doubleWriteAnalysis{
			props:   reg.props,
			overlay: reg.overlay,
			add:     addCallbacksWithID(st.add, reg.id),
			del:     deleteCallbacksWithID(st.del, reg.id),
		})
	}
	return out
}

func addCallbacksWithID(cur []addCallbackEntry, id uint64) []addCallbackEntry {
	if i := slices.IndexFunc(cur, func(e addCallbackEntry) bool { return e.id == id }); i >= 0 {
		return cur[i : i+1]
	}
	return nil
}

func deleteCallbacksWithID(cur []deleteCallbackEntry, id uint64) []deleteCallbackEntry {
	if i := slices.IndexFunc(cur, func(e deleteCallbackEntry) bool { return e.id == id }); i >= 0 {
		return cur[i : i+1]
	}
	return nil
}

func (s *Shard) analyzeForDoubleWrite(obj *storobj.Object, a doubleWriteAnalysis) ([]inverted.Property, error) {
	props, _, err := s.AnalyzeObjectForMigrationWithOverlay(obj, a.overlay)
	if err != nil {
		return nil, err
	}
	filtered := props[:0]
	for i := range props {
		if _, ok := a.props[props[i].Name]; ok {
			filtered = append(filtered, props[i])
		}
	}
	return filtered, nil
}

func (s *Shard) mirrorAddToIngest(st *propValueIndexState, docID uint64, obj *storobj.Object) error {
	for _, analysis := range st.analyses {
		props, err := s.analyzeForDoubleWrite(obj, analysis)
		if err != nil {
			return err
		}
		for i := range props {
			if err := s.fireAddToPropertyValueIndex(analysis.add, docID, &props[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Shard) mirrorDeleteFromIngest(st *propValueIndexState, docID uint64, obj *storobj.Object) error {
	for _, analysis := range st.analyses {
		props, err := s.analyzeForDoubleWrite(obj, analysis)
		if err != nil {
			return err
		}
		for i := range props {
			if err := s.fireDeleteFromPropertyValueIndex(analysis.del, docID, &props[i]); err != nil {
				return err
			}
		}
	}
	return nil
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
		if err := s.mirrorDeleteFromIngest(st, status.oldDocID, prevObject); err != nil {
			return err
		}
	}
	return s.mirrorAddToIngest(st, status.docID, object)
}

// migrationDoubleWriteDelete is migrationDoubleWrite's delete-only
// counterpart for the pure object-delete path.
func (s *Shard) migrationDoubleWriteDelete(st *propValueIndexState, prevObject *storobj.Object, docID uint64) error {
	if len(st.scope.props) == 0 || prevObject == nil {
		return nil
	}
	return s.mirrorDeleteFromIngest(st, docID, prevObject)
}

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

func (s *Shard) mutateScopeRegs(fn func(cur propValueIndexState) propValueIndexState) {
	var appeared []string
	s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
		standing := cur.conflicts
		cur = fn(cur)
		var conflicts []string
		cur.scope, conflicts = deriveScope(cur.scopeRegs)
		cur.overlaysDiverge = len(conflicts) > 0
		cur.conflicts = conflicts
		for _, prop := range conflicts {
			if !slices.Contains(standing, prop) {
				appeared = append(appeared, prop)
			}
		}
		return cur
	})
	for _, prop := range appeared {
		s.index.logger.WithField("property", prop).Warn(
			"two migrations mirror this property with different analyzer overlays; each is mirrored under its own")
	}
}
