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

package read

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (r *WeaviateReader) GetTenants(ctx context.Context, req mcp.CallToolRequest, args GetTenantsArgs) (*GetTenantsResp, error) {
	principal, err := r.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}
	tenants, err := r.schemaReader.GetConsistentTenants(ctx, principal, args.Collection, true, args.Tenants)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenants: %w", err)
	}
	return &GetTenantsResp{Tenants: tenants}, nil
}
