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

package backup

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/entities/backup"
)

type fakeSchemaManger struct {
	errRestoreClass error
	nodeName        string
	// Track NodeMapping passed to RestoreClass for testing
	lastNodeMapping map[string]string
}

func (f *fakeSchemaManger) RestoreClass(ctx context.Context, desc *backup.ClassDescriptor, nodeMapping map[string]string, overwriteAlias bool) error {
	f.lastNodeMapping = nodeMapping
	return f.errRestoreClass
}

func (f *fakeSchemaManger) NodeName() string {
	return f.nodeName
}

func TestFilterClasses(t *testing.T) {
	tests := []struct {
		in  []string
		xs  []string
		out []string
	}{
		{in: []string{}, xs: []string{}, out: []string{}},
		{in: []string{"a"}, xs: []string{}, out: []string{"a"}},
		{in: []string{"a"}, xs: []string{"a"}, out: []string{}},
		{in: []string{"1", "2", "3", "4"}, xs: []string{"2", "3"}, out: []string{"1", "4"}},
		{in: []string{"1", "2", "3"}, xs: []string{"1", "3"}, out: []string{"2"}},
		{in: []string{"1", "2", "1", "3", "1", "3"}, xs: []string{"2"}, out: []string{"1", "3"}},
	}
	for _, tc := range tests {
		got := filterClasses(tc.in, tc.xs)
		assert.ElementsMatch(t, tc.out, got)
	}
}

func TestHandlerValidateCoordinationOperation(t *testing.T) {
	var (
		ctx = context.Background()
		bm  = createManager(nil, nil, nil, nil)
	)

	{ // OnCanCommit
		req := Request{
			Method:   "Unknown",
			ID:       "1",
			Classes:  []string{"class1"},
			Backend:  "s3",
			Duration: time.Millisecond * 20,
			Bucket:   "bucket",
			Path:     "path",
		}
		resp := bm.OnCanCommit(ctx, &req)
		assert.Contains(t, resp.Err, "unknown backup operation")
		assert.Equal(t, resp.Timeout, time.Duration(0))
	}

	{ // OnCommit
		req := StatusRequest{
			Method:  "Unknown",
			ID:      "1",
			Backend: "s3",
		}
		err := bm.OnCommit(ctx, &req)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, errUnknownOp)
	}

	{ // OnAbort
		req := AbortRequest{
			Method: "Unknown",
			ID:     "1",
		}
		err := bm.OnAbort(ctx, &req)
		assert.NotNil(t, err)
		assert.ErrorIs(t, err, errUnknownOp)
	}
	{ // OnStatus
		req := StatusRequest{
			Method: "Unknown",
			ID:     "1",
		}
		ret := bm.OnStatus(ctx, &req)
		assert.Contains(t, ret.Err, errUnknownOp.Error())
	}
}

// TestCanCommitResponse_PreservesInFlightReindexErrorKind verifies that when
// the local sourcer (DB.Backupable) refuses with the
// "backup blocked: runtime-reindex in flight on this shard" sentinel
// message, OnCanCommit stamps CanCommitErrInFlightReindex on the response so
// the coordinator can promote it back to a typed error. Other refusal
// reasons must keep falling back to CanCommitErrCannotCommit.
func TestCanCommitResponse_PreservesInFlightReindexErrorKind(t *testing.T) {
	ctx := context.Background()
	backendName := "s3"

	tests := []struct {
		name        string
		backupErr   error
		wantContain string
		wantKind    CanCommitErrorKind
	}{
		{
			name: "in-flight reindex sentinel surfaces as CanCommitErrInFlightReindex",
			// Shape this exactly like reindexInFlightError() in
			// adapters/repos/db/reindex_inflight.go so the substring matcher
			// in classifyCanCommitErr exercises the realistic message.
			backupErr:   fmt.Errorf("Node-1/MyClass: %s: shard %q has 1 active tracker(s): ...; retry after the migration finishes", inFlightReindexSentinelMsg, "shard-a"),
			wantContain: inFlightReindexSentinelMsg,
			wantKind:    CanCommitErrInFlightReindex,
		},
		{
			name:        "generic refusal falls back to CanCommitErrCannotCommit",
			backupErr:   errors.New("unrelated boom"),
			wantContain: "unrelated boom",
			wantKind:    CanCommitErrCannotCommit,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := &fakeBackend{}
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/1")
			backend.On("GetObject", ctx, mock.Anything, BackupFile).Return(nil, errNotFound).Maybe()

			sourcer := &fakeSourcer{}
			sourcer.On("Backupable", ctx, mock.Anything).Return(tc.backupErr)

			bm := createManager(sourcer, nil, backend, nil)

			req := Request{
				Method:   OpCreate,
				ID:       "1",
				Classes:  []string{"MyClass"},
				Backend:  backendName,
				Duration: time.Millisecond * 20,
				Bucket:   "bucket",
				Path:     "path",
			}
			resp := bm.OnCanCommit(ctx, &req)

			assert.Contains(t, resp.Err, tc.wantContain)
			assert.Equal(t, tc.wantKind, resp.ErrKind)
			assert.Equal(t, time.Duration(0), resp.Timeout)
		})
	}
}
