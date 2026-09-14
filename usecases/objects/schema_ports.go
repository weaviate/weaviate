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

package objects

import (
	"context"

	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
)

// ClassResolver resolves collection aliases and reads classes through the schema
// manager's class cache, skipping authorization: callers must have authorized the
// request already. The objects layer and the gRPC batch path both use it.
type ClassResolver interface {
	GetCachedClassNoAuth(ctx context.Context, names ...string) (map[string]versioned.Class, error)
	local.AliasReader
}

// classGetter reads classes for an authorized request: the principal is checked,
// unlike the cache read in ClassResolver.
type classGetter interface {
	// GetClass returns the class, or nil when it does not exist.
	GetClass(ctx context.Context, principal *models.Principal, name string) (*models.Class, error)
	// GetCachedClass extracts the classes from the context, fetching them first if
	// the context does not carry them yet.
	GetCachedClass(ctx context.Context, principal *models.Principal, names ...string,
	) (map[string]versioned.Class, error)
}

// schemaManager is what the object read and write paths need of the schema use
// case: class reads, alias resolution, and the two things a write does first —
// activate the tenant it writes to, and wait for the schema version it was told.
type schemaManager interface {
	ClassResolver
	local.ClassReader
	local.UpdateWaiter
	classGetter

	// EnsureTenantActiveForWrite activates tenants when AutoTenantActivation is enabled.
	// Returns the schema version from activation. callers must use it in WaitForUpdate before writes.
	EnsureTenantActiveForWrite(ctx context.Context, class string, tenants ...string) (uint64, error)
}

// autoSchemaWriter is what the auto-schema path writes: it creates classes, upserts
// properties and adds tenants, then waits for its own writes to be applied locally.
type autoSchemaWriter interface {
	AddClass(ctx context.Context, principal *models.Principal, class *models.Class) (*models.Class, uint64, error)
	// AddClassProperty is an upsert: it adds properties to a class and updates
	// existing ones when merge is true.
	AddClassProperty(ctx context.Context, principal *models.Principal, className string, merge bool, prop ...*models.Property) (*models.Class, uint64, error)
	AddTenants(ctx context.Context, principal *models.Principal, class string, tenants []*models.Tenant) (uint64, error)
	local.UpdateWaiter
}
