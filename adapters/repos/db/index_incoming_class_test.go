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
	"encoding/json"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
)

// TestIncomingWritesCheckClass drives each remote and replica-prepare entry
// point that writes an object, reference or merge another node sent. Each must
// store only data of the index's own class.
func TestIncomingWritesCheckClass(t *testing.T) {
	const (
		ownClass     schema.ClassName = "alpha:Product"
		foreignClass schema.ClassName = "beta:Product"
		shardName                     = "t1"
		requestID                     = "req-1"
		id           strfmt.UUID      = "6f0b9a8e-2f43-4d4b-9a51-0c3c5e1a7f10"
	)
	ctx := context.Background()

	objs := func(classes []schema.ClassName) []*storobj.Object {
		out := make([]*storobj.Object, len(classes))
		for j, c := range classes {
			out[j] = storobj.FromObject(&models.Object{ID: id, Class: string(c)}, nil, nil, nil)
		}
		return out
	}
	// An empty class stands for a reference without a source.
	refs := func(classes []schema.ClassName) objects.BatchReferences {
		out := make(objects.BatchReferences, len(classes))
		for j, c := range classes {
			if c != "" {
				out[j].From = &crossref.RefSource{Class: c, Property: "hasItem", TargetID: id}
			}
		}
		return out
	}

	// The Incoming* paths write through a mock shard, which reports whether the
	// write reached it.
	mockShard := func(t *testing.T, idx *Index, method string, ret any) *bool {
		shard := NewMockShardLike(t)
		written := new(bool)
		shard.On("preventShutdown").Return(func() {}, nil).Maybe()
		shard.On(method, mock.Anything, mock.Anything).
			Run(func(mock.Arguments) { *written = true }).Return(ret).Maybe()
		idx.shards.Store(shardName, shard)
		return written
	}
	// The Replicate* paths only prepare, so a stored task is the write.
	preparingShard := func(idx *Index) func() bool {
		shard := &Shard{
			shutdownLock:   new(sync.RWMutex),
			replicationMap: pendingReplicaTasks{Tasks: map[string]replicaTask{}},
		}
		idx.shards.Store(shardName, shard)
		return func() bool {
			_, ok := shard.replicationMap.get(requestID)
			return ok
		}
	}

	// Err does not cross the wire, so read the response as the coordinator does.
	wire := func(t *testing.T, resp replica.SimpleResponse) error {
		b, err := json.Marshal(resp)
		require.NoError(t, err)
		var got replica.SimpleResponse
		require.NoError(t, json.Unmarshal(b, &got))
		return got.FirstError()
	}

	paths := []struct {
		name  string
		batch bool
		refs  bool
		// putObject on the coordinator refuses any other spelling of the class,
		// so these paths never receive one.
		skipOtherCase bool
		// write sends one payload item per class and returns the errors it got
		// back and whether the shard took the write.
		write func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool)
	}{
		{
			name:          "IncomingPutObject",
			skipOtherCase: true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				written := mockShard(t, idx, "PutObject", nil)
				err := idx.IncomingPutObject(ctx, shardName, objs(classes)[0], 0)
				return []error{err}, *written
			},
		},
		{
			name:  "IncomingBatchPutObjects",
			batch: true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				written := mockShard(t, idx, "PutObjectBatch", make([]error, len(classes)))
				errs := idx.IncomingBatchPutObjects(ctx, shardName, objs(classes), 0)
				require.Len(t, errs, len(classes))
				return errs, *written
			},
		},
		{
			name:  "IncomingBatchAddReferences",
			batch: true,
			refs:  true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				written := mockShard(t, idx, "AddReferencesBatch", make([]error, len(classes)))
				errs := idx.IncomingBatchAddReferences(ctx, shardName, refs(classes), 0)
				require.Len(t, errs, len(classes))
				return errs, *written
			},
		},
		{
			name: "IncomingMergeObject",
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				written := mockShard(t, idx, "MergeObject", nil)
				doc := objects.MergeDocument{Class: string(classes[0]), ID: id}
				return []error{idx.IncomingMergeObject(ctx, shardName, doc, 0)}, *written
			},
		},
		{
			name:          "ReplicateObject",
			skipOtherCase: true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				prepared := preparingShard(idx)
				resp := idx.ReplicateObject(ctx, shardName, requestID, objs(classes)[0], 0)
				return []error{wire(t, resp)}, prepared()
			},
		},
		{
			name:  "ReplicateObjects",
			batch: true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				prepared := preparingShard(idx)
				resp := idx.ReplicateObjects(ctx, shardName, requestID, objs(classes), 0)
				return []error{wire(t, resp)}, prepared()
			},
		},
		{
			name: "ReplicateUpdate",
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				prepared := preparingShard(idx)
				doc := &objects.MergeDocument{Class: string(classes[0]), ID: id}
				resp := idx.ReplicateUpdate(ctx, shardName, requestID, doc, 0)
				return []error{wire(t, resp)}, prepared()
			},
		},
		{
			name:  "ReplicateReferences",
			batch: true,
			refs:  true,
			write: func(t *testing.T, idx *Index, classes []schema.ClassName) ([]error, bool) {
				prepared := preparingShard(idx)
				resp := idx.ReplicateReferences(ctx, shardName, requestID, refs(classes), 0)
				return []error{wire(t, resp)}, prepared()
			},
		},
	}

	for _, p := range paths {
		newIndex := func(t *testing.T) *Index {
			return indexForGuardTest(t, string(ownClass), existerWithState(t, api.NamespaceStateActive))
		}

		t.Run(p.name+" writes its own class", func(t *testing.T) {
			// DB routes batch, reference and merge writes to an index by indexID,
			// so a coordinator also sends the class spelled in another case.
			class := schema.ClassName("alpha:PRODUCT")
			if p.skipOtherCase {
				class = ownClass
			}
			errs, written := p.write(t, newIndex(t), []schema.ClassName{class})
			for _, err := range errs {
				require.NoError(t, err)
			}
			assert.True(t, written)
		})

		t.Run(p.name+" refuses another namespace's class", func(t *testing.T) {
			payload := []schema.ClassName{foreignClass}
			if p.batch {
				// The foreign item goes second, so a check of only the first item admits it.
				payload = []schema.ClassName{ownClass, foreignClass}
			}
			errs, written := p.write(t, newIndex(t), payload)
			require.NotEmpty(t, errs)
			for _, err := range errs {
				require.ErrorContains(t, err, string(foreignClass))
			}
			assert.False(t, written)
		})

		if p.batch {
			t.Run(p.name+" refuses a batch whose first item is foreign", func(t *testing.T) {
				errs, written := p.write(t, newIndex(t), []schema.ClassName{foreignClass, ownClass})
				require.NotEmpty(t, errs)
				for _, err := range errs {
					require.ErrorContains(t, err, string(foreignClass))
				}
				assert.False(t, written)
			})
		}

		if p.refs {
			t.Run(p.name+" refuses a reference without a source", func(t *testing.T) {
				errs, written := p.write(t, newIndex(t), []schema.ClassName{ownClass, ""})
				require.NotEmpty(t, errs)
				for _, err := range errs {
					require.ErrorContains(t, err, "without a source")
				}
				assert.False(t, written)
			})
		}
	}
}
