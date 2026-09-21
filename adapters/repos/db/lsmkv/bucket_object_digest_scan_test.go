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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

const (
	digestScanUpdateTime = int64(1700000000000)
	digestScanOpTimeout  = 5 * time.Second
)

func digestScanKey(b byte) []byte { return bytes.Repeat([]byte{b}, 16) }

func digestScanHex(key []byte) string { return fmt.Sprintf("%x", key) }

func digestScanValue(t *testing.T, key []byte, docID uint64, updateTime int64) []byte {
	t.Helper()
	obj := storobj.FromObject(&models.Object{
		Class:              "Digest",
		ID:                 strfmt.UUID(uuid.UUID(key).String()),
		LastUpdateTimeUnix: updateTime,
	}, nil, nil, nil)
	obj.DocID = docID
	v, err := obj.MarshalBinary()
	require.NoError(t, err)
	return v
}

// newDigestScanBucket returns a Replace bucket and its once-guarded shutdown, also registered as cleanup.
func newDigestScanBucket(t *testing.T) (*Bucket, func() error) {
	t.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	var once sync.Once
	var shutdownErr error
	shutdown := func() error {
		once.Do(func() { shutdownErr = b.Shutdown(ctx) })
		return shutdownErr
	}
	t.Cleanup(func() { require.NoError(t, shutdown()) })
	return b, shutdown
}

func putDigestObject(t *testing.T, b *Bucket, key []byte, docID uint64, updateTime int64) {
	t.Helper()
	require.NoError(t, b.Put(key, digestScanValue(t, key, docID, updateTime)))
}

func parkFlush(t *testing.T, b *Bucket) {
	t.Helper()
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched)
	require.NotNil(t, b.flushing)
	t.Cleanup(func() { completeParkedFlush(t, b) })
}

func applyDigestScan(t *testing.T, b *Bucket) (map[string]int64, int) {
	t.Helper()
	folds := map[string]int64{}
	calls := 0
	err := b.NewObjectDigestScan().Apply(context.Background(), func() {}, func(uuidBytes []byte, updateTime int64) error {
		folds[digestScanHex(uuidBytes)] = updateTime
		calls++
		return nil
	})
	require.NoError(t, err)
	return folds, calls
}

func completesWithin(t *testing.T, d time.Duration, name string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err, name)
	case <-time.After(d):
		t.Fatalf("%s did not complete within %s", name, d)
	}
}

func TestObjectDigestScanVisitsEachLiveKeyOnce(t *testing.T) {
	keyA, keyB, keyC := digestScanKey(0x0a), digestScanKey(0x0b), digestScanKey(0x0c)
	hexA, hexB, hexC := digestScanHex(keyA), digestScanHex(keyB), digestScanHex(keyC)
	ts := digestScanUpdateTime

	tests := []struct {
		name   string
		layout func(t *testing.T, b *Bucket)
		want   map[string]int64
	}{
		{
			name: "memtableOnly",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
			},
			want: map[string]int64{hexA: ts, hexB: ts},
		},
		{
			name: "diskOnly",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
				require.NoError(t, b.FlushAndSwitch())
			},
			want: map[string]int64{hexA: ts, hexB: ts},
		},
		{
			name: "flushingOnly",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
				parkFlush(t, b)
			},
			want: map[string]int64{hexA: ts, hexB: ts},
		},
		{
			name: "straddleDiskAndActive",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				require.NoError(t, b.FlushAndSwitch())
				putDigestObject(t, b, keyA, 2, ts+1)
				putDigestObject(t, b, keyC, 3, ts)
			},
			want: map[string]int64{hexA: ts + 1, hexC: ts},
		},
		{
			name: "straddleFlushingAndActive",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				parkFlush(t, b)
				putDigestObject(t, b, keyA, 2, ts+1)
			},
			want: map[string]int64{hexA: ts + 1},
		},
		{
			name: "tombstoneOverDisk",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.Delete(keyB))
			},
			want: map[string]int64{hexA: ts},
		},
		{
			name: "tombstoneInFlushingOverDisk",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.Delete(keyB))
				parkFlush(t, b)
			},
			want: map[string]int64{hexA: ts},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := newDigestScanBucket(t)
			tc.layout(t, b)
			folds, calls := applyDigestScan(t, b)
			require.Equal(t, tc.want, folds)
			require.Equal(t, len(tc.want), calls)
		})
	}
}

func TestObjectDigestScanIgnoresPutAfterSnapshot(t *testing.T) {
	b, _ := newDigestScanBucket(t)
	keyA, keyX := digestScanKey(0x0a), digestScanKey(0x0e)
	ts := digestScanUpdateTime
	putDigestObject(t, b, keyA, 1, ts)
	valX := digestScanValue(t, keyX, 2, ts)

	scan := b.NewObjectDigestScan()

	inMemStarted := make(chan struct{})
	putIssued := make(chan struct{})
	putDone := make(chan error, 1)
	var putReturned atomic.Bool
	go func() {
		<-inMemStarted
		close(putIssued)
		err := b.Put(keyX, valX)
		putReturned.Store(true)
		putDone <- err
	}()

	var once sync.Once
	folds := map[string]int64{}
	callbacks := 0
	err := scan.Apply(context.Background(), func() { callbacks++ }, func(uuidBytes []byte, updateTime int64) error {
		once.Do(func() {
			close(inMemStarted)
			<-putIssued
			time.Sleep(50 * time.Millisecond)
			require.False(t, putReturned.Load(), "a put issued after the snapshot must block until the memtable pass ends")
		})
		folds[digestScanHex(uuidBytes)] = updateTime
		return nil
	})
	require.NoError(t, err)

	select {
	case err := <-putDone:
		require.NoError(t, err)
	case <-time.After(digestScanOpTimeout):
		t.Fatal("put did not complete after the scan released the memtable")
	}
	require.Equal(t, map[string]int64{digestScanHex(keyA): ts}, folds)
	require.Equal(t, 1, callbacks)
}

func TestObjectDigestScanCloseWithoutApplyReleasesLocks(t *testing.T) {
	b, shutdown := newDigestScanBucket(t)
	keyA, keyB := digestScanKey(0x0a), digestScanKey(0x0b)
	putDigestObject(t, b, keyA, 1, digestScanUpdateTime)
	valB := digestScanValue(t, keyB, 2, digestScanUpdateTime)

	scan := b.NewObjectDigestScan()
	scan.Close()
	scan.Close()

	completesWithin(t, digestScanOpTimeout, "put", func() error { return b.Put(keyB, valB) })
	completesWithin(t, digestScanOpTimeout, "flush", b.FlushAndSwitch)
	completesWithin(t, digestScanOpTimeout, "shutdown", shutdown)
}

func TestObjectDigestScanCancelMidScanReleasesCursors(t *testing.T) {
	keyA, keyB, keyC, keyD := digestScanKey(0x0a), digestScanKey(0x0b), digestScanKey(0x0c), digestScanKey(0x0d)
	ts := digestScanUpdateTime

	tests := []struct {
		name     string
		layout   func(t *testing.T, b *Bucket)
		cancelOn string
	}{
		{
			name: "cancelDuringInMemPass",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
			},
			cancelOn: digestScanHex(keyA),
		},
		{
			name: "cancelDuringDiskPass",
			layout: func(t *testing.T, b *Bucket) {
				putDigestObject(t, b, keyA, 1, ts)
				putDigestObject(t, b, keyB, 2, ts)
				require.NoError(t, b.FlushAndSwitch())
				putDigestObject(t, b, keyC, 3, ts)
			},
			cancelOn: digestScanHex(keyA),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, shutdown := newDigestScanBucket(t)
			tc.layout(t, b)
			valD := digestScanValue(t, keyD, 4, ts)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			callbacks := 0
			err := b.NewObjectDigestScan().Apply(ctx, func() { callbacks++ }, func(uuidBytes []byte, _ int64) error {
				if digestScanHex(uuidBytes) == tc.cancelOn {
					cancel()
				}
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, callbacks)

			completesWithin(t, digestScanOpTimeout, "put", func() error { return b.Put(keyD, valD) })
			completesWithin(t, digestScanOpTimeout, "flush", b.FlushAndSwitch)
			completesWithin(t, digestScanOpTimeout, "shutdown", shutdown)
		})
	}
}

func TestObjectDigestScanRejectsNonUUIDKey(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{name: "shorterThanUUID", key: "not-a-uuid"},
		{name: "longerThanUUID", key: "0123456789abcdef0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			active := newTestMemtableReplace(map[string][]byte{tc.key: []byte("value")})
			b := Bucket{active: active, disk: &SegmentGroup{}, strategy: StrategyReplace, logger: nullLogger()}

			folded := 0
			callbacks := 0
			err := b.NewObjectDigestScan().Apply(context.Background(), func() { callbacks++ }, func([]byte, int64) error {
				folded++
				return nil
			})
			require.ErrorContains(t, err, "invalid object uuid")
			require.Zero(t, folded)
			require.Equal(t, 1, callbacks)
		})
	}
}
