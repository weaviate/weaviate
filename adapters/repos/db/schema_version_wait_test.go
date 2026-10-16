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
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Test_DB_WaitsForSchemaVersion_BeforeIndexLookup pins the order of the two steps
// these entry points take: honour the schema version, then resolve the index by
// name. Resolving first cannot tell a collection this node has not applied yet
// apart from one that never existed, so a slow local apply becomes a failed write.
func Test_DB_WaitsForSchemaVersion_BeforeIndexLookup(t *testing.T) {
	const version uint64 = 7

	id := strfmt.UUID("d18c8e5e-a339-4c15-8af6-56b0cfe33ce7")

	entryPoints := []struct {
		name string
		// notFound is what the entry point reports once the wait is satisfied and
		// the collection is still absent.
		notFound string
		call     func(ctx context.Context, db *DB, schemaVersion uint64) error
	}{
		{
			name:     "PutObject",
			notFound: "non-existing index",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				return db.PutObject(ctx, &models.Object{Class: "Foo", ID: id},
					nil, nil, nil, nil, schemaVersion)
			},
		},
		{
			name:     "DeleteObject",
			notFound: "non-existing index",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				return db.DeleteObject(ctx, "Foo", id, time.Now(), nil, "", schemaVersion)
			},
		},
		{
			name:     "Merge",
			notFound: "non-existing index",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				return db.Merge(ctx, objects.MergeDocument{Class: "Foo", ID: id},
					nil, "", schemaVersion)
			},
		},
		{
			name:     "BatchPutObjects",
			notFound: "could not find index for class",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				batch := objects.BatchObjects{{
					OriginalIndex: 0,
					Object:        &models.Object{Class: "Foo", ID: id},
				}}
				res, err := db.BatchPutObjects(ctx, batch, nil, schemaVersion)
				if err != nil {
					return err
				}
				// An unresolvable collection is reported per object rather than as a
				// call error.
				return res[0].Err
			},
		},
		{
			name:     "BatchDeleteObjects",
			notFound: "cannot find index for class",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				_, err := db.BatchDeleteObjects(ctx, objects.BatchDeleteParams{ClassName: "Foo"},
					time.Now(), nil, "", schemaVersion)
				return err
			},
		},
		{
			name:     "AddBatchReferences",
			notFound: "could not find index for class",
			call: func(ctx context.Context, db *DB, schemaVersion uint64) error {
				// Two source collections, so a wait moved into the per-collection
				// loop would run twice and fail the mock's Once().
				refs := objects.BatchReferences{
					{
						From: &crossref.RefSource{Class: "Foo", Property: "ref", TargetID: id},
						To:   &crossref.Ref{Class: "Bar", TargetID: id},
					},
					{
						OriginalIndex: 1,
						From:          &crossref.RefSource{Class: "Baz", Property: "ref", TargetID: id},
						To:            &crossref.Ref{Class: "Bar", TargetID: id},
					},
				}
				res, err := db.AddBatchReferences(ctx, refs, nil, schemaVersion)
				if err != nil {
					return err
				}
				// An unresolvable source collection is reported per ref rather than
				// as a call error.
				return errors.Join(res[0].Err, res[1].Err)
			},
		},
	}

	tests := []struct {
		name    string
		version uint64
		waitErr error
		// wantErr defaults to the entry point's notFound when empty.
		wantErr string
	}{
		{
			name:    "local schema never catches up",
			version: version,
			waitErr: errors.New("deadline exceeded"),
			wantErr: "deadline exceeded",
		},
		{
			name:    "collection absent once the local schema caught up",
			version: version,
		},
		{
			name:    "unversioned write reaches the index lookup",
			version: 0,
		},
	}

	for _, ep := range entryPoints {
		for _, tt := range tests {
			t.Run(ep.name+"/"+tt.name, func(t *testing.T) {
				logger, _ := test.NewNullLogger()
				schemaReader := schemaUC.NewMockSchemaReader(t)
				schemaReader.EXPECT().WaitForUpdate(mock.Anything, tt.version).
					Return(tt.waitErr).Once()
				db := &DB{
					logger:       logger,
					indices:      map[string]*Index{},
					schemaReader: schemaReader,
					memMonitor:   memwatch.NewDummyMonitor(),
				}

				err := ep.call(context.Background(), db, tt.version)

				wantErr := tt.wantErr
				if wantErr == "" {
					wantErr = ep.notFound
				}
				require.ErrorContains(t, err, wantErr)
				if tt.waitErr != nil {
					require.NotContains(t, err.Error(), ep.notFound,
						"the wait must run before the index lookup")
				}
			})
		}
	}
}

// A replica write reports a failed wait over the wire, where Err is dropped
// (json:"-") and Msg is the only detail the coordinator can report.
func Test_DB_WaitForSchemaVersionForIndexWrite_ReportsFailureInMsg(t *testing.T) {
	const version uint64 = 7

	tests := []struct {
		name    string
		version uint64
		waitErr error
		wantMsg string
	}{
		{
			name:    "local schema never catches up",
			version: version,
			waitErr: errors.New("deadline exceeded"),
			wantMsg: "waiting for schema version 7: deadline exceeded",
		},
		{
			name:    "local schema caught up",
			version: version,
		},
		{
			name:    "unversioned write skips the wait",
			version: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaReader := schemaUC.NewMockSchemaReader(t)
			schemaReader.EXPECT().WaitForUpdate(mock.Anything, tt.version).
				Return(tt.waitErr).Once()
			db := &DB{schemaReader: schemaReader}

			resp := db.waitForSchemaVersionForIndexWrite(context.Background(), tt.version)

			if tt.wantMsg == "" {
				require.Nil(t, resp)
				return
			}
			require.Len(t, resp.Errors, 1)
			require.Equal(t, tt.wantMsg, resp.Errors[0].Msg)
			require.Equal(t, replicaerrors.StatusPreconditionFailed, resp.Errors[0].Code)
			require.Error(t, resp.Errors[0].Err)
		})
	}
}

// applyingSchemaReader holds className in the local schema at version while
// the apply that adds it has not finished. WaitForUpdate on that version runs
// finishApply, the rest of the apply, before it returns.
type applyingSchemaReader struct {
	schemaUC.SchemaReader
	className   string
	version     uint64
	finishApply func() error
}

func (r *applyingSchemaReader) ReadOnlyVersionedClass(name string) versioned.Class {
	if name != r.className {
		return versioned.Class{}
	}
	return versioned.Class{Class: &models.Class{Class: name}, Version: r.version}
}

func (r *applyingSchemaReader) WaitForUpdate(ctx context.Context, version uint64) error {
	if version != r.version {
		return r.SchemaReader.WaitForUpdate(ctx, version)
	}
	return r.finishApply()
}

const (
	applyingClass        = "BeingApplied"
	applyingClassVersion = 7
	applyingObjectID     = strfmt.UUID("8d5a3a0e-8a3d-4a8e-9f3c-2b7a4c1d6e5f")
)

// newApplyingDB returns a DB with one applyingClass object and its index left
// out of db.indices, as Migrator.AddClass leaves it until NewIndex returns.
// Waiting for schemaClass's version runs finishApply with the func that registers it.
func newApplyingDB(t *testing.T, schemaClass string, finishApply func(register func()) error) *DB {
	t.Helper()

	class := &models.Class{
		Class:               applyingClass,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
	}
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	obj := &models.Object{Class: applyingClass, ID: applyingObjectID, Properties: map[string]any{}}
	require.NoError(t, db.PutObject(t.Context(), obj, []float32{1, 2, 3}, nil, nil, nil, 0))

	idx := db.GetIndex(applyingClass)
	require.NotNil(t, idx)
	register := func() {
		db.indexLock.Lock()
		defer db.indexLock.Unlock()
		db.indices[idx.ID()] = idx
	}
	db.indexLock.Lock()
	delete(db.indices, idx.ID())
	db.indexLock.Unlock()
	// Shutdown closes only the indexes in db.indices.
	t.Cleanup(register)

	db.SetSchemaReader(&applyingSchemaReader{
		SchemaReader: db.schemaReader,
		className:    schemaClass,
		version:      applyingClassVersion,
		finishApply:  func() error { return finishApply(register) },
	})
	return db
}

// TestReadsWaitForTheIndexOfAClassBeingApplied reads a class whose index
// Migrator.AddClass has not registered yet. Each read waits for the class
// version, and every read but GetIndexForIncomingSharding returns a failed wait.
func TestReadsWaitForTheIndexOfAClassBeingApplied(t *testing.T) {
	logger, _ := test.NewNullLogger()

	reads := []struct {
		name string
		// read reports whether it reached the class's index.
		read func(ctx context.Context, db *DB) (bool, error)
		// waitErrReturned is false where the caller turns a failed wait into
		// the not-found answer it already gives.
		waitErrReturned bool
	}{
		{
			name: "Aggregate",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := db.Aggregate(ctx, aggregation.Params{ClassName: applyingClass, IncludeMetaCount: true}, nil)
				if err != nil {
					return false, err
				}
				return len(res.Groups) == 1, nil
			},
			waitErrReturned: true,
		},
		{
			name: "Search",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := db.Search(ctx, dto.GetParams{ClassName: applyingClass, Pagination: &filters.Pagination{Limit: 10}})
				return len(res) == 1, err
			},
			waitErrReturned: true,
		},
		{
			name: "VectorSearch",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := db.VectorSearch(ctx, dto.GetParams{ClassName: applyingClass, Pagination: &filters.Pagination{Limit: 10}},
					[]string{""}, []models.Vector{[]float32{1, 2, 3}})
				return len(res) == 1, err
			},
			waitErrReturned: true,
		},
		{
			name: "Query",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, qErr := db.Query(ctx, &objects.QueryInput{Class: applyingClass, Limit: 10})
				if qErr != nil {
					return false, qErr
				}
				return len(res) == 1, nil
			},
			waitErrReturned: true,
		},
		{
			name: "Object",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := db.Object(ctx, applyingClass, applyingObjectID, nil, additional.Properties{}, nil, "")
				return res != nil, err
			},
			waitErrReturned: true,
		},
		{
			name: "Exists",
			read: func(ctx context.Context, db *DB) (bool, error) {
				return db.Exists(ctx, applyingClass, applyingObjectID, nil, "")
			},
			waitErrReturned: true,
		},
		{
			name: "FetchObject",
			read: func(ctx context.Context, db *DB) (bool, error) {
				shards, err := db.schemaReader.Shards(applyingClass)
				if err != nil {
					return false, err
				}
				res, err := db.FetchObject(ctx, applyingClass, shards[0], applyingObjectID)
				return res.Object != nil, err
			},
			waitErrReturned: true,
		},
		{
			name: "GetIndexForIncomingSharding",
			read: func(ctx context.Context, db *DB) (bool, error) {
				return db.GetIndexForIncomingSharding(ctx, applyingClass) != nil, nil
			},
			waitErrReturned: false,
		},
		{
			name: "GetShardsStatus",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := NewMigrator(db, logger, "node1").GetShardsStatus(ctx, applyingClass, "")
				return len(res) == 1, err
			},
			waitErrReturned: true,
		},
		{
			name: "GetShardsQueueSize",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := NewMigrator(db, logger, "node1").GetShardsQueueSize(ctx, applyingClass, "")
				return len(res) == 1, err
			},
			waitErrReturned: true,
		},
		{
			name: "LocalNodeStatus",
			read: func(ctx context.Context, db *DB) (bool, error) {
				res, err := db.LocalNodeStatus(ctx, applyingClass, "", verbosity.OutputVerbose)
				if err != nil {
					return false, err
				}
				return len(res.Shards) == 1, nil
			},
			waitErrReturned: true,
		},
	}

	errWait := errors.New("schema version not applied")
	applies := []struct {
		name        string
		finishApply func(register func()) error
		wantReached bool
		wantErr     bool
	}{
		{
			name:        "apply finishes during the wait",
			finishApply: func(register func()) error { register(); return nil },
			wantReached: true,
		},
		{
			name:        "wait fails",
			finishApply: func(func()) error { return errWait },
			wantErr:     true,
		},
	}

	for _, apply := range applies {
		for _, r := range reads {
			t.Run(apply.name+"/"+r.name, func(t *testing.T) {
				db := newApplyingDB(t, applyingClass, apply.finishApply)

				reached, err := r.read(t.Context(), db)

				require.Equal(t, apply.wantReached, reached)
				if apply.wantErr && r.waitErrReturned {
					require.ErrorContains(t, err, errWait.Error())
					// Query maps a failed wait to 500, never to its 404 for a missing class.
					var objErr *objects.Error
					if errors.As(err, &objErr) {
						require.Equal(t, objects.StatusInternalServerError, objErr.Code)
					}
					return
				}
				require.NoError(t, err)
			})
		}
	}
}

// TestIndexForReadReportsAMissingIndexWithoutError looks up a class whose index
// no pending apply will register. indexForRead returns a nil index and no error.
func TestIndexForReadReportsAMissingIndexWithoutError(t *testing.T) {
	tests := []struct {
		name string
		// schemaClass is the class the local schema holds.
		schemaClass string
	}{
		{name: "class missing from the local schema", schemaClass: "OtherClass"},
		{name: "class applied but its index never built", schemaClass: applyingClass},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newApplyingDB(t, tt.schemaClass, func(func()) error { return nil })

			idx, err := db.indexForRead(t.Context(), applyingClass)

			require.NoError(t, err)
			require.Nil(t, idx)
		})
	}
}
