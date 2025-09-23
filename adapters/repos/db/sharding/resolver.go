//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package resolver provides shard resolution strategies for Weaviate collections
// based on their configuration. It maps objects to target shards using either
// UUID-based consistent hashing (single-tenant) or tenant-based partitioning
// (multi-tenant), enabling efficient distribution and retrieval of data.
package resolver

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/weaviate/weaviate/adapters/repos/db/multitenancy"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// ShardTarget represents the computed shard destination for a single object.
// It couples the target shard name with the original object to maintain
// the association through the shard resolution pipeline.
type ShardTarget struct {
	Shard  string
	Object *storobj.Object
}

// ShardTargets represents a collection of shard targets for batch processing.
// It provides utility methods for grouping and manipulation of shard resolution
// results.
type ShardTargets []*ShardTarget

// ShardBatches groups objects by their target shard for efficient batch processing.
// The map keys are shard names, and values are slices of objects assigned to that shard.
type ShardBatches map[string][]*storobj.Object

// GroupByShard groups objects by their target shard for batch processing.
// This method enables efficient per-shard operations by collecting all objects
// targeting the same shard into a single slice.
//
// The returned map is not ordered and reuses the input object pointers for
// memory efficiency. Each shard name maps to a slice of objects that should
// be processed by that shard.
//
// Returns a ShardBatches map where keys are shard names and values are
// slices of objects targeting that shard.
func (t ShardTargets) GroupByShard() ShardBatches {
	groups := make(ShardBatches)
	for _, target := range t {
		groups[target.Shard] = append(groups[target.Shard], target.Object)
	}
	return groups
}

// Shards returns the unique shard names present in the targets.
// This method extracts all distinct shard names from the resolution results,
// which is useful for determining which shards need to be contacted for
// a batch operation.
//
// The returned slice contains each shard name exactly once, but the ordering
// is unspecified and may vary between calls.
//
// Returns a slice of unique shard names.
func (t ShardTargets) Shards() []string {
	seen := make(map[string]struct{})
	var shards []string
	for _, target := range t {
		if _, ok := seen[target.Shard]; !ok {
			seen[target.Shard] = struct{}{}
			shards = append(shards, target.Shard)
		}
	}
	return shards
}

// Len returns the number of target shards in the batch of shard targets.
//
// Returns the number of target shards.
func (t ShardTargets) Len() int { return len(t) }

// schemaReader provides access to schema operations required by shard resolvers.
// It abstracts the underlying schema storage to enable testing and provides a
// minimal interface focused on shard resolution needs.
type schemaReader interface {
	// ShardFromUUID returns the target shard name for a class and UUID bytes
	// using consistent hashing. This enables deterministic shard assignment
	// based on object UUIDs.
	ShardFromUUID(className string, uuidBytes []byte) string

	// TenantsShards returns tenant status information keyed by the tenant name.
	// The tenant names match shard names in multi-tenant configurations.
	// This method supports bulk tenant lookups for performance optimization.
	TenantsShards(ctx context.Context, className string, tenantNames ...string) (map[string]string, error)

	// ReadOnlyClass returns the class metadata for the specified class name.
	// Returns nil if the class does not exist in the schema.
	ReadOnlyClass(className string) *models.Class
}

// byUUIDShardResolver implements shard resolution using consistent hashing of object UUIDs.
// This strategy is used for single-tenant collections where objects are distributed
// across shards based on their UUID to achieve balanced data distribution and
// consistent routing for the same object across requests.
type byUUIDShardResolver struct {
	className       string
	schemaReader    schemaReader
	tenantValidator *multitenancy.TenantValidator
}

// ResolveShardByObjectID resolves the target shard for an object using its UUID and tenant information.
// This method performs tenant validation to ensure single-tenant constraints,
// then uses consistent hashing to deterministically map the UUID to a shard.
//
// For single-tenant collections, the tenant parameter must be empty and the objectID
// is used for consistent hashing to determine the target shard.
//
// Parameters:
//   - ctx: request context for validation operations
//   - objectID: the UUID of the object to resolve a shard for
//   - tenant: tenant name (must be empty for single-tenant collections)
//
// Returns the target shard name, or an error if validation fails or UUID parsing fails.
func (r *byUUIDShardResolver) ResolveShardByObjectID(ctx context.Context, objectID strfmt.UUID, tenant string) (string, error) {
	if err := r.tenantValidator.ValidateTenants(ctx, tenant); err != nil {
		return "", err
	}

	parsedUUID, err := uuid.Parse(objectID.String())
	if err != nil {
		return "", fmt.Errorf("parse uuid %q: %w", objectID.String(), err)
	}
	uuidBytes, err := parsedUUID.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal uuid %q: %w", objectID.String(), err)
	}

	return r.schemaReader.ShardFromUUID(r.className, uuidBytes), nil
}

// newByUUIDShardResolver creates a UUID-based shard resolver for a collection.
// The resolver integrates single-tenant validation directly rather than injecting it,
// since UUID-based resolution inherently requires single-tenant validation (rejecting
// any tenant parameters). This design eliminates unnecessary abstraction and prevents
// misconfiguration with inappropriate validation logic.
//
// Parameters:
//   - className: the name of the class this resolver will handle
//   - schemaReader: provides access to schema operations for UUID-to-shard mapping
//
// Returns a configured byUUIDShardResolver.
func newByUUIDShardResolver(className string, schemaReader schemaReader) *byUUIDShardResolver {
	return &byUUIDShardResolver{
		className:       className,
		schemaReader:    schemaReader,
		tenantValidator: multitenancy.NewTenantValidator(className, false, schemaReader),
	}
}

// ResolveShard resolves the target shard for an object using its UUID.
// This method performs tenant validation to ensure single-tenant constraints,
// then uses consistent hashing to deterministically map the UUID to a shard.
//
// The UUID is parsed and converted to its binary form for consistent hashing.
// Validation errors are passed through unchanged to enable mapping to specific
// HTTP status codes, while parsing errors are wrapped with context.
//
// Parameters:
//   - ctx: request context for validation operations
//   - object: the object to resolve a shard for
//
// Returns the ShardTarget containing the shard name and object, or an error
// if validation fails or UUID parsing fails.
func (r *byUUIDShardResolver) ResolveShard(ctx context.Context, object *storobj.Object) (*ShardTarget, error) {
	tenant := object.Object.Tenant
	if err := r.tenantValidator.ValidateTenants(ctx, tenant); err != nil {
		return nil, err
	}

	id := object.ID()
	parsedUUID, err := uuid.Parse(id.String())
	if err != nil {
		return nil, fmt.Errorf("parse uuid %q: %w", id.String(), err)
	}
	uuidBytes, err := parsedUUID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal uuid %q: %w", id.String(), err)
	}

	return &ShardTarget{
		Shard:  r.schemaReader.ShardFromUUID(r.className, uuidBytes),
		Object: object,
	}, nil
}

// ResolveShards resolves shard targets for multiple objects using UUID-based routing.
// This method processes each object independently using ResolveShard, which results
// in individual validation and UUID parsing for each object.
//
// Processing stops on the first error, and the error is returned as-is to preserve
// the original error context for proper error handling by callers.
//
// Parameters:
//   - ctx: request context for validation and parsing operations
//   - objects: slice of objects to resolve shards for
//
// Returns ShardTargets containing all successfully resolved targets, or an error
// if any object fails validation or parsing.
func (r *byUUIDShardResolver) ResolveShards(ctx context.Context, objects []*storobj.Object) (ShardTargets, error) {
	targets := make(ShardTargets, 0, len(objects))
	for _, object := range objects {
		targetShard, err := r.ResolveShard(ctx, object)
		if err != nil {
			return nil, err
		}
		targets = append(targets, targetShard)
	}
	return targets, nil
}

// byTenantShardResolver implements shard resolution using tenant names as shard identifiers.
// This strategy is used for multi-tenant collections where each tenant's data is
// isolated in its own shard, with the tenant name directly mapping to the shard name.
type byTenantShardResolver struct {
	className       string
	schemaReader    schemaReader
	tenantValidator *multitenancy.TenantValidator
}

// ResolveShardByObjectID resolves the target shard for an object using tenant-based routing.
// In multi-tenant configurations, the tenant name directly maps to the shard name,
// providing perfect tenant isolation. The objectID parameter is not used for shard
// resolution but is kept for interface consistency.
//
// Validation ensures the tenant is provided and active. Validation errors are
// passed through unchanged to enable proper HTTP status code mapping.
//
// Parameters:
//   - ctx: request context for validation operations
//   - objectID: the UUID of the object (not used for shard resolution in multi-tenant)
//   - tenant: tenant name which becomes the shard name
//
// Returns the target shard name (tenant name), or an error if validation fails.
func (r *byTenantShardResolver) ResolveShardByObjectID(ctx context.Context, _ strfmt.UUID, tenant string) (string, error) {
	if err := r.tenantValidator.ValidateTenants(ctx, tenant); err != nil {
		return "", err
	}

	return tenant, nil
}

// newByTenantShardResolver creates a tenant-based shard resolver for a collection.
// The resolver integrates multi-tenant validation directly rather than injecting it,
// since tenant-based resolution inherently requires multi-tenant validation (ensuring
// tenant parameters are provided and valid). This design eliminates unnecessary
// abstraction and prevents misconfiguration with inappropriate validation logic.
//
// Parameters:
//   - className: the name of the class this resolver will handle
//   - schemaReader: provides access to schema operations for tenant validation
//
// Returns a configured byTenantShardResolver.
func newByTenantShardResolver(className string, schemaReader schemaReader) *byTenantShardResolver {
	return &byTenantShardResolver{
		className:       className,
		schemaReader:    schemaReader,
		tenantValidator: multitenancy.NewTenantValidator(className, true, schemaReader),
	}
}

// ResolveShard resolves the target shard for an object using its tenant name.
// In multi-tenant configurations, the tenant name directly maps to the shard name,
// providing perfect tenant isolation.
//
// Validation ensures the tenant is provided and active. Validation errors are
// passed through unchanged to enable proper HTTP status code mapping.
//
// Parameters:
//   - ctx: request context for validation operations
//   - object: the object to resolve a shard for
//
// Returns the ShardTarget with tenant name as shard name, or an error if
// validation fails.
func (r *byTenantShardResolver) ResolveShard(ctx context.Context, object *storobj.Object) (*ShardTarget, error) {
	tenant := object.Object.Tenant
	if err := r.tenantValidator.ValidateTenants(ctx, tenant); err != nil {
		return nil, err
	}
	return &ShardTarget{Shard: tenant, Object: object}, nil
}

// ResolveShards resolves shard targets for multiple objects using tenant-based routing.
// This method is optimized for batch operations by extracting unique tenant names
// and performing bulk validation all tenants, significantly reducing schema operations
// compared to individual ResolveShard calls.
//
// The optimization reduces both schema round trips and validation overhead:
// - Single bulk tenant validation instead of multiple individual validations
// - Efficient tenant extraction with deduplication
//
// Validation errors are returned unchanged while schema errors are wrapped with context.
//
// Parameters:
//   - ctx: request context for validation operations
//   - objects: slice of objects to resolve shards for
//
// Returns ShardTargets containing all resolved targets, or an error if validation
// or schema operations fail.
func (r *byTenantShardResolver) ResolveShards(ctx context.Context, objects []*storobj.Object) (ShardTargets, error) {
	if len(objects) == 0 {
		return ShardTargets{}, nil
	}

	tenants := r.extractUniqueTenants(objects)
	if err := r.tenantValidator.ValidateTenants(ctx, tenants...); err != nil {
		return nil, err
	}

	targets := make(ShardTargets, 0, len(objects))
	for _, object := range objects {
		targets = append(targets, &ShardTarget{
			Shard:  object.Object.Tenant,
			Object: object,
		})
	}
	return targets, nil
}

// extractUniqueTenants extracts unique tenant names from a collection of objects.
// This method performs deduplication to minimize the work required for bulk
// tenant validation and schema operations.
//
// The order of returned tenant names is unspecified and may vary between calls.
//
// Parameters:
//   - objects: slice of objects to extract tenant names from
//
// Returns a slice of unique tenant names found in the objects.
func (r *byTenantShardResolver) extractUniqueTenants(objects []*storobj.Object) []string {
	tenantSet := make(map[string]bool)
	for _, object := range objects {
		tenantSet[object.Object.Tenant] = true
	}
	tenants := make([]string, 0, len(tenantSet))
	for tenant := range tenantSet {
		tenants = append(tenants, tenant)
	}
	return tenants
}

// ShardResolver provides a unified interface for shard resolution regardless of
// the collection's multi-tenancy configuration. It delegates to the appropriate
// resolution strategy (UUID-based or tenant-based) selected at construction time.
type ShardResolver struct {
	resolveShard           func(ctx context.Context, object *storobj.Object) (*ShardTarget, error)
	resolveShards          func(ctx context.Context, objects []*storobj.Object) (ShardTargets, error)
	resolveShardByObjectID func(ctx context.Context, objectID strfmt.UUID, tenant string) (string, error)
}

// ResolveShardByObjectID resolves the target shard for an object using its UUID and tenant information.
// The resolution strategy (UUID-based or tenant-based) depends on the collection's
// multi-tenancy configuration determined at construction time.
//
// For single-tenant collections, shard determination uses consistent hashing of
// the object's UUID and the tenant parameter must be empty. For multi-tenant collections,
// the tenant name directly maps to the shard name and the objectID is not used for resolution.
//
// Errors from the underlying resolution strategy are passed through unchanged
// to preserve error context for proper handling by callers.
//
// Parameters:
//   - ctx: request context for validation and schema operations
//   - objectID: the UUID of the object to resolve a shard for
//   - tenant: tenant name (empty for single-tenant, required for multi-tenant)
//
// Returns the target shard name, or an error if resolution fails.
func (r *ShardResolver) ResolveShardByObjectID(ctx context.Context, objectID strfmt.UUID, tenant string) (string, error) {
	return r.resolveShardByObjectID(ctx, objectID, tenant)
}

// ResolveShard resolves the target shard for a single object.
// The resolution strategy (UUID-based or tenant-based) depends on the collection's
// multi-tenancy configuration determined at construction time.
//
// For single-tenant collections, shard determination uses consistent hashing of
// the object's UUID. For multi-tenant collections, the object's tenant name
// directly maps to the shard name.
//
// Errors from the underlying resolution strategy are passed through unchanged
// to preserve error context for proper handling by callers.
//
// Parameters:
//   - ctx: request context for validation and schema operations
//   - object: the object to resolve a shard for
//
// Returns the ShardTarget containing the resolved shard name and object,
// or an error if resolution fails.
func (r *ShardResolver) ResolveShard(ctx context.Context, object *storobj.Object) (*ShardTarget, error) {
	return r.resolveShard(ctx, object)
}

// ResolveShards resolves target shards for multiple objects efficiently.
// The method delegates to the appropriate batch resolution strategy, which
// may include optimizations like bulk validation for multi-tenant collections.
//
// Errors from the underlying strategy are passed through unchanged to preserve
// error context and enable proper error handling by callers.
//
// Parameters:
//   - ctx: request context for validation and schema operations
//   - objects: slice of objects to resolve shards for
//
// Returns ShardTargets containing all resolved shard targets, or an error
// if resolution fails for any object.
func (r *ShardResolver) ResolveShards(ctx context.Context, objects []*storobj.Object) (ShardTargets, error) {
	return r.resolveShards(ctx, objects)
}

// NewShardResolver constructs a ShardResolver with the appropriate resolution strategy
// based on the collection's multi-tenancy configuration.
//
// If multi-tenancy is enabled, a tenant-based strategy is used where objects
// are mapped to shards based on their tenant name. Otherwise, a UUID-based
// strategy is used where objects are mapped to shards using consistent hashing
// of their UUID.
//
// Parameters:
//   - className: the name of the class to resolve shards for
//   - multiTenancyEnabled: whether the class has multi-tenancy enabled
//   - schemaReader: provides access to schema operations
//
// Returns a configured ShardResolver that uses the appropriate strategy.
func NewShardResolver(className string, multiTenancyEnabled bool, schemaReader schemaReader) *ShardResolver {
	if multiTenancyEnabled {
		resolver := newByTenantShardResolver(className, schemaReader)
		return &ShardResolver{
			resolveShard:           resolver.ResolveShard,
			resolveShards:          resolver.ResolveShards,
			resolveShardByObjectID: resolver.ResolveShardByObjectID,
		}
	}
	resolver := newByUUIDShardResolver(className, schemaReader)
	return &ShardResolver{
		resolveShard:           resolver.ResolveShard,
		resolveShards:          resolver.ResolveShards,
		resolveShardByObjectID: resolver.ResolveShardByObjectID,
	}
}

// resolverStrategy defines the interface that all shard resolution strategies must implement.
// This interface ensures consistency across different resolution approaches and enables
// compile-time verification of strategy implementations.
type resolverStrategy interface {
	ResolveShard(ctx context.Context, object *storobj.Object) (*ShardTarget, error)
	ResolveShards(ctx context.Context, objects []*storobj.Object) (ShardTargets, error)
	ResolveShardByObjectID(ctx context.Context, objectID strfmt.UUID, tenant string) (string, error)
}

// Interface compliance checks at compile time.
var (
	_ resolverStrategy = (*byUUIDShardResolver)(nil)
	_ resolverStrategy = (*byTenantShardResolver)(nil)
	_ resolverStrategy = (*ShardResolver)(nil)
)
