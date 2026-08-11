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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/memwatch"
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
