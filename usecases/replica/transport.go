//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package replica

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/replica"
	"github.com/semi-technologies/weaviate/entities/storobj"
	"github.com/semi-technologies/weaviate/usecases/objects"
)

type ReplicationClient interface {
	PutObject(ctx context.Context, host, index, shard, requestID string,
		obj *storobj.Object) (replica.SimpleResponse, error)
	DeleteObject(ctx context.Context, host, index, shard, requestID string,
		id strfmt.UUID) (replica.SimpleResponse, error)
	PutObjects(ctx context.Context, host, index, shard, requestID string,
		objs []*storobj.Object) (replica.SimpleResponse, error)
	MergeObject(ctx context.Context, host, index, shard, requestID string,
		mergeDoc *objects.MergeDocument) (replica.SimpleResponse, error)
	DeleteObjects(ctx context.Context, host, index, shard, requestID string,
		docIDs []uint64, dryRun bool) (replica.SimpleResponse, error)
	AddReferences(ctx context.Context, host, index, shard, requestID string,
		refs objects.BatchReferences) (replica.SimpleResponse, error)
	Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error
	Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error)
}
