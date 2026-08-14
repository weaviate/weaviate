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

package read

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (r *WeaviateReader) GetTenants(ctx context.Context, req mcp.CallToolRequest, args GetTenantsArgs) (resp *GetTenantsResp, retErr error) {
	log := r.logger.WithFields(logrus.Fields{
		"tool":       "weaviate-tenants-list",
		"collection": args.CollectionName,
	})
	log.Debug("listing tenants")

	// Tool-level authz only. GetConsistentTenants resolves the class name and
	// applies per-tenant RBAC via its ShardsMetadata filter, so an additional
	// CollectionsData pre-check here would both double-resolve the name and
	// gate on the wrong permission (read_data vs. read_tenants).
	principal, err := r.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = namespacing.StripErrForPrincipal(principal, retErr) }()

	tenants, err := r.schemaReader.GetConsistentTenants(ctx, principal, args.CollectionName, true, nil)
	if err != nil {
		log.Warnf("failed to get tenants: %v", err)
		return nil, fmt.Errorf("failed to get tenants: %w", err)
	}
	return &GetTenantsResp{Tenants: tenants}, nil
}
