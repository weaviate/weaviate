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
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset/keydoccolumn"
	"github.com/weaviate/weaviate/entities/concurrency"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// keyDocColumnFlattenSlots bounds how many key/doc columns rebuild their base at
// once in this process. It scales with CPUs because rebuilds are CPU-bound, and
// no benchmark sets it.
var keyDocColumnFlattenSlots = max(2, concurrency.GOMAXPROCS_2)

var keyDocColumnFlattenSem = make(chan struct{}, keyDocColumnFlattenSlots)

// acquireKeyDocColumnFlattenSlot takes a rebuild slot if one is free, without
// waiting. A rebuild refused one is left pending, like one refused memory.
func acquireKeyDocColumnFlattenSlot() (func(), bool) {
	select {
	case keyDocColumnFlattenSem <- struct{}{}:
		return func() { <-keyDocColumnFlattenSem }, true
	default:
		return nil, false
	}
}

// reportKeyDocColumnSize moves lsm_key_doc_column_size by this group's change
// since its last report, to zero when it has no column. The column is loaded under
// keyDocColumnSizeMu, so a report cannot re-add bytes a concurrent drop took out.
func (sg *SegmentGroup) reportKeyDocColumnSize() {
	sg.keyDocColumnSizeMu.Lock()
	defer sg.keyDocColumnSizeMu.Unlock()

	now := 0
	if idx := sg.keyDocColumn.Load(); idx != nil {
		now = idx.Info().SizeBytes
	}
	sg.metrics.AddKeyDocColumnSize(int64(now - sg.keyDocColumnSizeReported))
	sg.keyDocColumnSizeReported = now
}

// startPendingKeyDocColumnFlatten restarts a rebuild the column deferred, on
// the column's own goroutine so it never holds up this group's compaction.
func (sg *SegmentGroup) startPendingKeyDocColumnFlatten() {
	if idx := sg.keyDocColumn.Load(); idx != nil {
		idx.StartPendingFlatten()
	}
}

// dropKeyDocColumn detaches and closes the column, so a flattening in flight
// stops, and takes its bytes out of the gauge.
func (sg *SegmentGroup) dropKeyDocColumn() {
	if idx := sg.keyDocColumn.Swap(nil); idx != nil {
		idx.Close()
	}
	sg.reportKeyDocColumnSize()
}

// keyDocColumnDeclined holds the column names the not-built warning has fired for.
// keyDocColumnDetached holds the column names the flush-detach warning has fired for.
var (
	keyDocColumnDeclined sync.Map
	keyDocColumnDetached sync.Map
)

func logKeyDocColumnDecline(logger logrus.FieldLogger, name string, err error) {
	keydoccolumn.WarnOncePerName(&keyDocColumnDeclined, logger, name,
		"key/doc column not built, ContainsAny queries stay on the standard path until "+
			"the shard is loaded again (node restart or tenant reactivation): %v", err)
}

// checkAlloc reports whether allocating that many more bytes is safe, or nil if
// this group has no memory monitor to ask — which is what a test gets.
func (sg *SegmentGroup) checkAlloc(bytes int64) error {
	if sg.allocChecker == nil {
		return nil
	}
	return sg.allocChecker.CheckAlloc(bytes)
}

// initKeyDocColumn builds the index from the disk segments alone while the group
// is constructed, before any flush. Unflushed data is layered over it at query
// time, so including the recovered memtable would count it twice.
func (sg *SegmentGroup) initKeyDocColumn(name string, docIDCountGetter roaringset.DocIDCountGetterFunc) error {
	before := time.Now()
	// A lazy segment loads here and panics if it cannot. The panic is recovered,
	// so the bucket still opens, without a column.
	var cursors []roaringset.InnerCursor
	release := func() {}
	if err := enterrors.RunRecovered(sg.logger, func() error {
		cursors, release = sg.newRoaringSetCursors()
		return nil
	}); err != nil {
		return fmt.Errorf("read the segments: %w", err)
	}
	defer release()

	merged := roaringset.NewCombinedCursor(cursors, false, concurrency.SROAR_MERGE)
	idx, err := keydoccolumn.BuildFromCursor(merged, keydoccolumn.BuildOptions{
		Name:       name,
		DocIDCount: docIDCountGetter,
		// carrying the bucket, so the index's own warnings name the property
		Logger: sg.logger.WithField("bucket", filepath.Base(sg.dir)),
		// Buckets build concurrently at startup, and a build allocates far more
		// than it keeps.
		CheckAlloc: sg.checkAlloc,
		// a flattening that finds the index unfit has no caller to return to
		OnUnfit: sg.dropKeyDocColumn,
		// A flattening changes the size without a flush, so an idle bucket would
		// otherwise keep the figure from before the flattening.
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

// mergeMemtableIntoKeyDocColumn feeds a sealed, just-flushed memtable to the
// index before the swap; [Bucket.addFlushedSegment] explains the order. A failed
// merge detaches the index, which would otherwise miss those documents.
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
