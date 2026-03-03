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

package clients

import (
	"context"
	"sync"
	"time"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// restOnlyTTL is how long a host is cached as REST-only after receiving Unimplemented.
	restOnlyTTL = 60 * time.Second
)

// hybridReplicationClient wraps a gRPC client and a REST client.
// When gRPC is enabled, it attempts gRPC first. On codes.Unimplemented
// (indicating an old node), it falls back to REST and caches that host.
type hybridReplicationClient struct {
	grpcClient *grpcReplicationClient
	restClient *replicationClient
	enabled    func() bool // runtime check for REPLICATION_GRPC_ENABLED

	mu           sync.RWMutex
	restOnlyHosts map[string]time.Time // host -> expiry
}

// NewHybridReplicationClient creates a hybrid client that attempts gRPC first
// (when enabled) and falls back to REST on Unimplemented errors.
func NewHybridReplicationClient(
	grpcClient *grpcReplicationClient,
	restClient *replicationClient,
	enabled func() bool,
) *hybridReplicationClient {
	return &hybridReplicationClient{
		grpcClient:    grpcClient,
		restClient:    restClient,
		enabled:       enabled,
		restOnlyHosts: make(map[string]time.Time),
	}
}

// useGRPC returns true if gRPC should be attempted for the given host.
func (h *hybridReplicationClient) useGRPC(host string) bool {
	if !h.enabled() {
		return false
	}

	h.mu.RLock()
	expiry, found := h.restOnlyHosts[host]
	h.mu.RUnlock()

	if !found {
		return true
	}

	if time.Now().After(expiry) {
		// TTL expired, remove from cache and try gRPC again.
		h.mu.Lock()
		delete(h.restOnlyHosts, host)
		h.mu.Unlock()
		return true
	}

	return false
}

// markRESTOnly caches a host as REST-only for restOnlyTTL.
func (h *hybridReplicationClient) markRESTOnly(host string) {
	h.mu.Lock()
	h.restOnlyHosts[host] = time.Now().Add(restOnlyTTL)
	h.mu.Unlock()
}

// isUnimplemented checks if the error is a gRPC Unimplemented status.
func isUnimplemented(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unimplemented
}

// ── Write operations (WClient) ───────────────────────────────────────────────

func (h *hybridReplicationClient) PutObject(ctx context.Context, host, index, shard, requestID string,
	obj *storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.PutObject(ctx, host, index, shard, requestID, obj, schemaVersion)
}

func (h *hybridReplicationClient) PutObjects(ctx context.Context, host, index, shard, requestID string,
	objs []*storobj.Object, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.PutObjects(ctx, host, index, shard, requestID, objs, schemaVersion)
}

func (h *hybridReplicationClient) MergeObject(ctx context.Context, host, index, shard, requestID string,
	doc *objects.MergeDocument, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.MergeObject(ctx, host, index, shard, requestID, doc, schemaVersion)
}

func (h *hybridReplicationClient) DeleteObject(ctx context.Context, host, index, shard, requestID string,
	id strfmt.UUID, deletionTime time.Time, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.DeleteObject(ctx, host, index, shard, requestID, id, deletionTime, schemaVersion)
}

func (h *hybridReplicationClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.DeleteObjects(ctx, host, index, shard, requestID, uuids, deletionTime, dryRun, schemaVersion)
}

func (h *hybridReplicationClient) AddReferences(ctx context.Context, host, index, shard, requestID string,
	refs []objects.BatchReference, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.AddReferences(ctx, host, index, shard, requestID, refs, schemaVersion)
}

func (h *hybridReplicationClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	if h.useGRPC(host) {
		err := h.grpcClient.Commit(ctx, host, index, shard, requestID, resp)
		if !isUnimplemented(err) {
			return err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.Commit(ctx, host, index, shard, requestID, resp)
}

func (h *hybridReplicationClient) Abort(ctx context.Context, host, index, shard, requestID string) (replica.SimpleResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.Abort(ctx, host, index, shard, requestID)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.Abort(ctx, host, index, shard, requestID)
}

// ── Read operations (RClient) ────────────────────────────────────────────────

func (h *hybridReplicationClient) FetchObject(ctx context.Context, host, index, shard string,
	id strfmt.UUID, props search.SelectProperties, additional additional.Properties, numRetries int,
) (replica.Replica, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.FetchObject(ctx, host, index, shard, id, props, additional, numRetries)
}

func (h *hybridReplicationClient) FetchObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID,
) ([]replica.Replica, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.FetchObjects(ctx, host, index, shard, ids)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.FetchObjects(ctx, host, index, shard, ids)
}

func (h *hybridReplicationClient) DigestObjects(ctx context.Context, host, index, shard string,
	ids []strfmt.UUID, numRetries int,
) ([]types.RepairResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.DigestObjects(ctx, host, index, shard, ids, numRetries)
}

func (h *hybridReplicationClient) DigestObjectsInRange(ctx context.Context, host, index, shard string,
	initialUUID, finalUUID strfmt.UUID, limit int,
) ([]types.RepairResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.DigestObjectsInRange(ctx, host, index, shard, initialUUID, finalUUID, limit)
}

func (h *hybridReplicationClient) OverwriteObjects(ctx context.Context, host, index, shard string,
	vobjects []*objects.VObject,
) ([]types.RepairResponse, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.OverwriteObjects(ctx, host, index, shard, vobjects)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.OverwriteObjects(ctx, host, index, shard, vobjects)
}

func (h *hybridReplicationClient) FindUUIDs(ctx context.Context, host, index, shard string,
	filter *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.FindUUIDs(ctx, host, index, shard, filter, limit)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.FindUUIDs(ctx, host, index, shard, filter, limit)
}

func (h *hybridReplicationClient) HashTreeLevel(ctx context.Context, host, index, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	if h.useGRPC(host) {
		resp, err := h.grpcClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
		if !isUnimplemented(err) {
			return resp, err
		}
		h.markRESTOnly(host)
	}
	return h.restClient.HashTreeLevel(ctx, host, index, shard, level, discriminant)
}
