package read

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

func (r *WeaviateReader) GetTenants(ctx context.Context, req mcp.CallToolRequest, args GetTenantsArgs) (*GetTenantsResp, error) {
	principal, err := r.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get principal: %w", err)
	}
	tenants, err := r.schemaReader.GetConsistentTenants(ctx, principal, args.Collection, true, args.Tenants)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenants: %w", err)
	}
	return &GetTenantsResp{Tenants: tenants}, nil
}
