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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

type fakeSchemaManger struct {
	errRestoreClass     error
	nodeName            string
	lastNodeMapping     map[string]string
	lastStripNamespaces bool
	namespacesEnabled   bool
	liveEntities        []string
}

func (f *fakeSchemaManger) RestoreClass(ctx context.Context, desc *backup.ClassDescriptor, nodeMapping map[string]string, overwriteAlias bool, stripNamespaces bool) error {
	f.lastNodeMapping = nodeMapping
	f.lastStripNamespaces = stripNamespaces
	return f.errRestoreClass
}

func (f *fakeSchemaManger) NodeName() string {
	return f.nodeName
}

func (f *fakeSchemaManger) NamespacesEnabled() bool {
	return f.namespacesEnabled
}

func (f *fakeSchemaManger) ClassEqual(name string) string {
	for _, n := range f.liveEntities {
		if strings.EqualFold(n, name) {
			return n
		}
	}
	return ""
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

func TestServerVersionOlderThan(t *testing.T) {
	tests := []struct {
		serverVersion string
		want          bool
	}{
		{serverVersion: "1.9", want: true},
		{serverVersion: "1.100", want: false},
		{serverVersion: "1.22.3", want: true},
		{serverVersion: "1.23", want: false},
		{serverVersion: "1.23.0", want: false},
		{serverVersion: "1.16", want: true},
		{serverVersion: "2.0", want: false},
		{serverVersion: "0.9", want: true},
		{serverVersion: "", want: false},
		{serverVersion: "garbage", want: false},
		{serverVersion: "1", want: false},
		{serverVersion: "1.x", want: false},
		{serverVersion: "v1.22", want: false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q", tc.serverVersion), func(t *testing.T) {
			assert.Equal(t, tc.want, serverVersionOlderThan(tc.serverVersion, 1, 23))
		})
	}
}

func TestCheckRestorableVersion(t *testing.T) {
	const current = "1.23"
	tests := []struct {
		version       string
		serverVersion string
		wantErr       error
		wantMsg       string
	}{
		{version: "1.0", serverVersion: current, wantErr: errLegacyUncompressed},
		{version: "1", serverVersion: current, wantErr: errLegacyUncompressed},
		{version: "0.9", serverVersion: current, wantErr: errLegacyUncompressed},
		{version: "2.0", serverVersion: current},
		{version: "2.1", serverVersion: current},
		{version: "2.9", serverVersion: current},
		{version: "3.0", serverVersion: current, wantMsg: errMsgHigherVersion},
		// A structure version may omit the minor; the major still decides.
		{version: "3", serverVersion: current, wantMsg: errMsgHigherVersion},
		{version: "10", serverVersion: current, wantMsg: errMsgHigherVersion},
		// A byte compare read this as older than "2.1" and wrongly accepted it.
		{version: "10.0", serverVersion: current, wantMsg: errMsgHigherVersion},
		// A corrupt descriptor is reported by Validate, not refused as an old format.
		{version: "", serverVersion: current},
		// The version this build writes must stay restorable by it.
		{version: Version, serverVersion: current},

		{version: Version, serverVersion: "1.22", wantErr: errLegacyFlatFS},
		{version: Version, serverVersion: "1.16", wantErr: errLegacyFlatFS},
		// A lexical compare read "1.100" as older than "1.23" and would have refused it.
		{version: Version, serverVersion: "1.100"},
		{version: Version, serverVersion: "2.0"},
		// An unreadable server version is never classified as an old format.
		{version: Version, serverVersion: ""},
		{version: Version, serverVersion: "garbage"},
		// Both clauses match; the format the version names is reported first.
		{version: "1.0", serverVersion: "1.16", wantErr: errLegacyUncompressed},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%q/%q", tc.version, tc.serverVersion), func(t *testing.T) {
			err := checkRestorableVersion(tc.version, tc.serverVersion)
			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.wantMsg != "":
				require.ErrorContains(t, err, tc.wantMsg)
			default:
				require.NoError(t, err)
			}
		})
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
// "backup blocked: runtime-reindex in flight" sentinel
// message, OnCanCommit stamps CanCommitErrInFlightReindex on the response so
// the coordinator can promote it back to a typed error. Other refusal
// reasons must keep falling back to CanCommitErrCannotCommit.
//
// The restore arm is here too: it refuses on a different input (the
// cluster-wide gate rather than the per-class sourcer) but lands on the same
// three response fields, and it additionally has to refuse before the
// descriptor is read.
func TestCanCommitResponse_PreservesInFlightReindexErrorKind(t *testing.T) {
	ctx := context.Background()
	backendName := "s3"

	tests := []struct {
		name string
		// method defaults to OpCreate when empty.
		method Op
		// backupErr is what the per-class sourcer refuses with; gateErr is what
		// the cluster-wide restore gate refuses with. A row sets one.
		backupErr   error
		gateErr     error
		wantContain string
		// wantExactErr, when set, pins the whole message rather than a
		// substring of it.
		wantExactErr string
		wantKind     CanCommitErrorKind
		// wantDescriptorRead says whether the participant is allowed to have
		// read the backup descriptor before answering.
		wantDescriptorRead bool
	}{
		{
			name: "in-flight reindex sentinel surfaces as CanCommitErrInFlightReindex",
			// Shape this exactly like reindexInFlightError() in
			// adapters/repos/db/reindex_inflight.go: wrap the shared
			// backup.ErrBackupBlockedByInFlightReindex sentinel so the
			// errors.Is-based classifier in classifyCanCommitErr matches.
			backupErr: fmt.Errorf("Node-1/MyClass: %w: shard %q has 1 active tracker(s): ...; retry after the migration finishes",
				backup.ErrBackupBlockedByInFlightReindex, "shard-a"),
			wantContain: backup.ErrBackupBlockedByInFlightReindex.Error(),
			wantKind:    CanCommitErrInFlightReindex,
		},
		{
			name: "in-flight sentinel inside errors.Join is still classified (mirrors DB.Backupable shape)",
			// DB.Backupable accumulates per-class refusals via errors.Join.
			// errors.Is must walk the joined graph; substring matching would
			// trip on this realistic case.
			backupErr: errors.Join(
				fmt.Errorf("Node-1/ClassA: %w: shard %q (collection %q): ...",
					backup.ErrBackupBlockedByInFlightReindex, "shard-a", "ClassA"),
				fmt.Errorf("Node-1/ClassB: %w: shard %q (collection %q): ...",
					backup.ErrBackupBlockedByInFlightReindex, "shard-b", "ClassB"),
			),
			wantContain: backup.ErrBackupBlockedByInFlightReindex.Error(),
			wantKind:    CanCommitErrInFlightReindex,
		},
		{
			name:        "generic refusal falls back to CanCommitErrCannotCommit",
			backupErr:   errors.New("unrelated boom"),
			wantContain: "unrelated boom",
			wantKind:    CanCommitErrCannotCommit,
		},
		{
			// A restore is refused by the cluster-wide gate, and it has to be
			// refused before restorer.validate reads the descriptor. The
			// per-shard backup kind would re-materialize the message under the
			// backup sentinel; this kind carries the cluster-wide one.
			name:    "restore refused by the cluster-wide gate",
			method:  OpRestore,
			gateErr: gateRefusal(),
			wantExactErr: "restore blocked: runtime-reindex in flight in the cluster: " +
				"retry after the migration finishes",
			wantKind: CanCommitErrRestoreBlockedByReindex,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The backend answers rather than rejecting, so a dropped gate
			// fails an assertion instead of panicking in the mock.
			backend := newFakeBackend()
			backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return("bucket/backups/1")
			backend.On("GetObject", mock.Anything, mock.Anything, mock.Anything).Return(nil, errNotFound).Maybe()

			sourcer := &fakeSourcer{reindexInFlightErr: tc.gateErr}
			sourcer.On("Backupable", ctx, mock.Anything).Return(tc.backupErr).Maybe()

			bm := createManager(sourcer, nil, backend, nil)

			method := tc.method
			if method == "" {
				method = OpCreate
			}
			req := Request{
				Method:   method,
				ID:       "1",
				Classes:  []string{"MyClass"},
				Backend:  backendName,
				Duration: time.Millisecond * 20,
				Bucket:   "bucket",
				Path:     "path",
			}
			resp := bm.OnCanCommit(ctx, &req)

			if tc.wantExactErr != "" {
				assert.Equal(t, tc.wantExactErr, resp.Err)
			} else {
				assert.Contains(t, resp.Err, tc.wantContain)
			}
			assert.Equal(t, tc.wantKind, resp.ErrKind)
			assert.Equal(t, time.Duration(0), resp.Timeout)
			if !tc.wantDescriptorRead {
				backend.AssertNotCalled(t, "GetObject", mock.Anything, mock.Anything, mock.Anything)
			}
		})
	}
}

// gateRefusal mirrors the error shape DB.RefuseIfAnyReindexInFlight returns.
func gateRefusal() error {
	return fmt.Errorf("%w: retry after the migration finishes", backup.ErrReindexInFlight)
}

// canCommitGatedRestore asks a participant whose cluster-wide gate is shut to
// take on a restore.
func canCommitGatedRestore(backend *fakeBackend) *CanCommitResponse {
	sourcer := &fakeSourcer{}
	sourcer.reindexInFlightErr = gateRefusal()

	return createManager(sourcer, nil, backend, nil).OnCanCommit(context.Background(), &Request{
		Method:   OpRestore,
		ID:       "1",
		Classes:  []string{"MyClass"},
		Backend:  "s3",
		Duration: time.Millisecond * 20,
	})
}

// TestOnCanCommitRestore_WordingSurvivesRoundTrip pins that a restore refusal
// still reads as one after the coordinator rebuilds it from the RPC response.
func TestOnCanCommitRestore_WordingSurvivesRoundTrip(t *testing.T) {
	err := canCommitErrFromResponse(canCommitGatedRestore(newFakeBackend()))
	require.ErrorIs(t, err, backup.ErrReindexInFlight,
		"the sentinel must survive the RPC, or the coordinator answers 500 instead of 422")

	got := err.Error()
	assert.Equal(t, "restore blocked: runtime-reindex in flight in the cluster: "+
		"retry after the migration finishes", got,
		"the sentinel rides along for errors.Is; it must not be printed twice")
	assert.NotContains(t, got, "backup blocked",
		"a restore refusal must not be worded as a backup refusal")
	assert.NotContains(t, got, "this shard",
		"the gate is cluster-wide; no shard is involved")
}
