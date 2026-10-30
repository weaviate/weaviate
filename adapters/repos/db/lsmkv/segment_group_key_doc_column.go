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
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset/keydoccolumn"
	"github.com/weaviate/weaviate/entities/concurrency"
)

// keyDocColumnFlattenSlots bounds how many key/doc columns rebuild their base
// at once in this process.
//
// Single-flight is per column, and there is one column per configured property
// per shard — ten thousand tenants, ten thousand columns, every one of them
// able to start at the same moment. Each rebuild holds a whole second base
// while it runs, and they cannot see each other through the memory check: that
// reads a heap figure refreshed on a timer, which the runtime itself only
// updates at a collection, so every column asking inside one window gets the
// same answer. Two is a figure to start from rather than a measured one: it
// makes the worst case two extra bases instead of one per tenant.
const keyDocColumnFlattenSlots = 2

var keyDocColumnFlattenSem = make(chan struct{}, keyDocColumnFlattenSlots)

// acquireKeyDocColumnFlattenSlot takes a rebuild slot if one is free, without
// waiting. A rebuild that cannot have one is deferred and retried by the index
// itself, which is the same disposition as the node having no memory for it.
func acquireKeyDocColumnFlattenSlot() (func(), bool) {
	select {
	case keyDocColumnFlattenSem <- struct{}{}:
		return func() { <-keyDocColumnFlattenSem }, true
	default:
		return nil, false
	}
}

// reportKeyDocColumnSize moves the node's total by whatever this bucket's column
// has gained or lost since it last said anything — nothing, if it has no column,
// which is how a bucket subtracts itself back out when its column is dropped or
// its segment group shuts down.
//
// Called from the paths that change the number, so it costs a load and a
// subtraction on work that has just done far more than that. Nothing samples it
// on a timer; there is no goroutine here.
//
// The read and the write of what was last reported are one step, so two reports
// landing at once compose to the right total. Taken separately, a flush
// computing a size and a concurrent detach storing zero can land in an order
// that credits the node with a column nobody holds, for as long as the process
// runs.
func (sg *SegmentGroup) reportKeyDocColumnSize() {
	var now keydoccolumn.Info
	if idx := sg.keyDocColumn.Load(); idx != nil {
		now = idx.Info()
	}

	sg.keyDocColumnSizeMu.Lock()
	defer sg.keyDocColumnSizeMu.Unlock()

	was := sg.keyDocColumnSizeReported
	sg.keyDocColumnSizeReported = now
	sg.metrics.AddKeyDocColumnSize(
		int64(now.BaseBytes-was.BaseBytes),
		int64(now.OverflowBytes-was.OverflowBytes),
		int64(now.LayersBytes-was.LayersBytes))
}

// dropKeyDocColumn detaches the column, if this group has one, and takes its
// bytes back out of the node's total.
//
// Closing it is what stops a flattening still rebuilding a base for an owner
// that has stopped pointing at the index — a copy of the whole key column, on
// something already on its way out. Every drop goes through here so that no
// site has to remember the pair.
func (sg *SegmentGroup) dropKeyDocColumn() {
	if idx := sg.keyDocColumn.Swap(nil); idx != nil {
		idx.Close()
	}
	sg.reportKeyDocColumnSize()
}

// checkAlloc reports whether allocating that many more bytes is safe, or nil if
// this group has no memory monitor to ask — which is what a test gets.
func (sg *SegmentGroup) checkAlloc(bytes int64) error {
	if sg.allocChecker == nil {
		return nil
	}
	return sg.allocChecker.CheckAlloc(bytes)
}

// initKeyDocColumn builds and attaches the index, once, while the segment group
// is being constructed — before the bucket exists, so before any flush, which
// leaves the flush hook a target from the start.
//
// It reads the disk segments alone. Not because there is nothing else to read —
// a memtable recovered from the commit log is already in place by now — but
// because whatever is unflushed is layered over the index at query time, so a
// base that included it would hold those documents twice and depend on the
// memtables happening to be empty here.
func (sg *SegmentGroup) initKeyDocColumn(docIDCountGetter roaringset.DocIDCountGetterFunc) error {
	if docIDCountGetter == nil {
		return errors.New("no docID count getter given")
	}

	before := time.Now()
	cursors, release := sg.newRoaringSetCursors()
	defer release()

	merged := roaringset.NewCombinedCursor(cursors, false, concurrency.SROAR_MERGE)
	idx, err := keydoccolumn.BuildFromCursor(merged, keydoccolumn.BuildOptions{
		DocIDCount: docIDCountGetter,
		// carrying the bucket, so everything this index says afterwards — a build
		// over values that are not unique, a flush it cannot absorb, a flattening
		// it gives up on — names the property it is about. It says all of it from
		// its own goroutine, where nothing else could attach that.
		Logger: sg.logger.WithField("bucket", filepath.Base(sg.dir)),
		// Buckets open concurrently at startup, so several of these can run at
		// once, and a build allocates far more in passing than it keeps. The
		// rangeable representation refuses on pressure and serves from disk; this
		// one refuses and leaves ContainsAny on the standard fold.
		CheckAlloc: sg.checkAlloc,
		// A flattening is the one place that can find the index no longer worth
		// holding, and it runs on its own goroutine with nowhere to return that
		// to. Dropping it is this side's decision, and it is the same store the
		// failed merge below makes.
		OnUnfit: sg.dropKeyDocColumn,
		// A flattening is the one thing that changes what this index holds
		// without a flush, and a bucket whose writes have stopped takes no
		// further flush — so without this the figure a node is sized from keeps
		// the pre-flatten value for the life of the process.
		OnResize:           sg.reportKeyDocColumnSize,
		AcquireFlattenSlot: acquireKeyDocColumnFlattenSlot,
	})
	if err != nil {
		return err
	}

	sg.keyDocColumn.Store(idx)
	sg.reportKeyDocColumnSize()
	info := idx.Info()
	sg.logger.WithFields(logrus.Fields{
		"action":      "keydoccolumn_build",
		"bucket":      filepath.Base(sg.dir),
		"took":        time.Since(before).String(),
		"keys":        info.Keys,
		"key_width":   info.KeyWidth,
		"docid_width": info.DocIDWidth,
		"size_mb":     fmt.Sprintf("%.3f", float64(info.SizeBytes)/1024/1024),
	}).Debug("key/doc column built")
	return nil
}

// mergeMemtableIntoKeyDocColumn feeds a just-flushed, sealed memtable to the
// index. It must run before the segment swap and outside flushLock; the call
// site in [Bucket.FlushAndSwitch] carries the ordering argument.
//
// If the index declines the flush, it is detached. The layer is appended only
// after every check that can fail, so a failure leaves the index without the
// flushed documents — and once the swap lands, those documents live in the new
// segment alone, which the index never reads. Keeping it would mean answering
// ContainsAny with silently missing results.
func (sg *SegmentGroup) mergeMemtableIntoKeyDocColumn(flushing memtable) error {
	idx := sg.keyDocColumn.Load()
	if idx == nil {
		return nil
	}
	// sealed by waitForZeroWriters: out of active use and its writers have
	// drained, so the index reads it without paying for a copy it cannot use
	if err := idx.MergeMemtableByCursor(flushing.newSealedRoaringSetCursor()); err != nil {
		sg.dropKeyDocColumn()
		return err
	}
	// Every flush, since that is what grows it. A flattening's shrink does not
	// wait for the next one: it reports through OnResize.
	sg.reportKeyDocColumnSize()
	return nil
}
