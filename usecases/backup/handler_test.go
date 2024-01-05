//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// helper methods
func (m *Handler) Backup(ctx context.Context, pr *models.Principal, req *BackupRequest,
) (*models.BackupCreateResponse, error) {
	store, err := nodeBackend(m.node, m.backends, req.Backend, req.ID)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}

	classes := req.Include
	if err := store.Initialize(ctx); err != nil {
		return nil, backup.NewErrUnprocessable(fmt.Errorf("init uploader: %w", err))
	}
	if meta, err := m.backupper.Backup(ctx, store, req.ID, classes); err != nil {
		return nil, err
	} else {
		status := string(meta.Status)
		return &models.BackupCreateResponse{
			Classes: classes,
			ID:      req.ID,
			Backend: req.Backend,
			Status:  &status,
			Path:    meta.Path,
		}, nil
	}
}

func (m *Handler) Restore(ctx context.Context, pr *models.Principal,
	req *BackupRequest,
) (*models.BackupRestoreResponse, error) {
	store, err := nodeBackend(m.node, m.backends, req.Backend, req.ID)
	if err != nil {
		err = fmt.Errorf("no backup backend %q, did you enable the right module?", req.Backend)
		return nil, backup.NewErrUnprocessable(err)
	}
	meta, err := m.validateRestoreRequest(ctx, store, req)
	if err != nil {
		return nil, err
	}
	cs := meta.List()
	// if cls := m.restorer.AnyExists(cs); cls != "" {
	// 	err := fmt.Errorf("cannot restore class %q because it already exists", cls)
	// 	return nil, backup.NewErrUnprocessable(err)
	// }
	rreq := Request{
		Method:      OpRestore,
		ID:          meta.ID,
		Backend:     req.Backend,
		Classes:     cs,
		NodeMapping: req.NodeMapping,
	}
	data, err := m.restorer.Restore(ctx, &rreq, meta, store)
	if err != nil {
		return nil, backup.NewErrUnprocessable(err)
	}

	return data, nil
}

func (m *Handler) validateRestoreRequest(ctx context.Context, store nodeStore, req *BackupRequest) (*backup.BackupDescriptor, error) {
	meta, cs, err := m.restorer.validate(ctx, &store, &Request{ID: req.ID, Classes: req.Include})
	if err != nil {
		if errors.Is(err, errMetaNotFound) {
			return nil, backup.NewErrNotFound(err)
		}
		return nil, backup.NewErrUnprocessable(err)
	}
	meta.Exclude(req.Exclude)
	if len(meta.Classes) == 0 {
		err = fmt.Errorf("empty class list: please choose from : %v", cs)
		return nil, backup.NewErrUnprocessable(err)
	}
	return meta, nil
}

type fakeSchemaManger struct {
	errRestoreClass error
	nodeName        string
}

func (f *fakeSchemaManger) RestoreClass(context.Context, *backup.ClassDescriptor, map[string]string,
) error {
	return f.errRestoreClass
}

func (f *fakeSchemaManger) NodeName() string {
	return f.nodeName
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
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
