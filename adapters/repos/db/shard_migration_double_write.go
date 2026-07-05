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
	"sync/atomic"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/storobj"
)

// migrationDoubleWriteScope names the properties whose live double-write must
// be analyzed under the migration's TARGET schema instead of the source
// schema, plus the analyzer overlay that produces that target analysis. It is
// empty (nil maps) whenever no reindex migration is ingesting on this shard,
// which is the overwhelmingly common case; a nil-map lookup on the write path
// then costs nothing.
//
// The scope is DERIVED from the registered ingest-window double-write
// callbacks, never persisted: OnAfterLsmInit re-registers the callbacks on
// every boot and re-arms the scope, so a restart rebuilds it with no on-disk
// format.
type migrationDoubleWriteScope struct {
	props   map[string]struct{}
	overlay map[string]inverted.PropertyOverlay
}

// withArmed returns a copy-on-write scope that additionally covers props and
// merges overlay. Callers hold propertyValueIndexCallbacksMu; the returned
// maps are fresh so a concurrent lock-free reader keeps observing the old
// snapshot until the atomic Store publishes this one.
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

// withDisarmed returns a copy-on-write scope with props and overlay's keys
// removed. An emptied scope collapses back to nil maps so the write path's
// fast path (len(scope.props) == 0) re-engages once the migration ends.
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

// propValueIndexState is the single copy-on-write snapshot behind the write
// path's one atomic Load: the add/delete property-value-index callback slices
// PLUS the migration double-write scope. Folding all three into one value is
// what makes a write observe a CONSISTENT triple — a concurrent arm/disarm can
// never expose "callbacks registered but scope not yet armed" (a source-schema
// leak into the ingest bucket) or "scope armed but callbacks gone" (a target
// write silently dropped).
type propValueIndexState struct {
	add   []onAddToPropertyValueIndex
	del   []onDeleteFromPropertyValueIndex
	scope migrationDoubleWriteScope
}

// emptyPropValueIndexState is returned by loadPropValueIndexState before any
// callback has ever been registered, so callers never nil-check the Load.
var emptyPropValueIndexState = &propValueIndexState{}

// loadPropValueIndexState returns the current folded snapshot without locking.
// The whole write path takes exactly one of these per object so inline
// suppression, migAdd/migDel computation, and the migration callback pass all
// read the same consistent {add,del,scope}.
func (s *Shard) loadPropValueIndexState() *propValueIndexState {
	if v := s.propValueIndexState.Load(); v != nil {
		return v.(*propValueIndexState)
	}
	return emptyPropValueIndexState
}

// migrationDoubleWriteScope exposes just the scope of the current snapshot for
// callers that do not need the callback slices. The hot write path uses the
// full state's .scope instead so it Loads only once.
func (s *Shard) migrationDoubleWriteScope() *migrationDoubleWriteScope {
	return &s.loadPropValueIndexState().scope
}

// mutatePropValueIndexState is the SOLE writer of the folded snapshot: it
// applies fn to a copy under propertyValueIndexCallbacksMu and publishes the
// result with one atomic Store, so registration, arm, and disarm each land as
// a single indivisible transition for lock-free readers. fn must copy any
// slice/map it grows rather than appending in place.
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

func appendAddCallback(cur []onAddToPropertyValueIndex, cb onAddToPropertyValueIndex) []onAddToPropertyValueIndex {
	updated := make([]onAddToPropertyValueIndex, len(cur)+1)
	copy(updated, cur)
	updated[len(cur)] = cb
	return updated
}

func appendDeleteCallback(cur []onDeleteFromPropertyValueIndex, cb onDeleteFromPropertyValueIndex) []onDeleteFromPropertyValueIndex {
	updated := make([]onDeleteFromPropertyValueIndex, len(cur)+1)
	copy(updated, cur)
	updated[len(cur)] = cb
	return updated
}

// fireAddToPropertyValueIndex invokes every registered add callback against
// the given (already target-analyzed) property. The migration pass calls this
// directly to BYPASS the scope suppression that the inline write path applies —
// suppressing here would drop the very write the migration exists to make.
func (s *Shard) fireAddToPropertyValueIndex(st *propValueIndexState, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range st.add {
		ec.Add(cb(s, docID, property))
	}
	return ec.ToError()
}

// fireDeleteFromPropertyValueIndex is the delete-side counterpart of
// fireAddToPropertyValueIndex.
func (s *Shard) fireDeleteFromPropertyValueIndex(st *propValueIndexState, docID uint64, property *inverted.Property) error {
	ec := errorcompounder.New()
	for _, cb := range st.del {
		ec.Add(cb(s, docID, property))
	}
	return ec.ToError()
}

// analyzeForDoubleWrite produces the TARGET-schema analysis of obj that the
// migration double-write needs: AnalyzeObjectForMigrationWithOverlay (which
// captures RawValues and applies the Force*/tokenization overlay) restricted
// to the properties currently in migration scope. Non-scope properties are
// dropped so the migration pass never touches a bucket it does not own.
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

// migrationDoubleWrite mirrors an object write into the reindex ingest bucket
// under the migration's TARGET analysis, for the scope properties whose inline
// callback the write path suppressed. It deletes the previous object's target
// terms then adds the new object's, matching the main path's delete-then-add
// order; the ingest bucket is a write-only sidecar until swap, so the full
// re-add per write is idempotent and invisible to queries.
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

// registerDoubleWriteWithScope registers the ingest-window add+delete
// double-write callbacks AND arms the migration scope in ONE atomic Store, so
// a concurrent writer never sees the callbacks without the scope (which would
// leak source-tokenized terms into the ingest bucket — the weaviate#298 bug).
// The returned disable func tears both down again.
func (s *Shard) registerDoubleWriteWithScope(add onAddToPropertyValueIndex, del onDeleteFromPropertyValueIndex,
	props []string, overlay map[string]inverted.PropertyOverlay,
) func() {
	disabled := &atomic.Bool{}
	wrappedAdd := func(shard *Shard, docID uint64, property *inverted.Property) error {
		if disabled.Load() {
			return nil
		}
		return add(shard, docID, property)
	}
	wrappedDelete := func(shard *Shard, docID uint64, property *inverted.Property) error {
		if disabled.Load() {
			return nil
		}
		return del(shard, docID, property)
	}

	s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
		cur.add = appendAddCallback(cur.add, wrappedAdd)
		cur.del = appendDeleteCallback(cur.del, wrappedDelete)
		cur.scope = cur.scope.withArmed(props, overlay)
		return cur
	})

	return func() {
		// Stop the callbacks first (lock-free) so they cannot write to the
		// post-swap ingest bucket, then drop the scope so the inline path
		// resumes owning these props.
		disabled.Store(true)
		s.mutatePropValueIndexState(func(cur propValueIndexState) propValueIndexState {
			cur.scope = cur.scope.withDisarmed(props, overlay)
			return cur
		})
	}
}
