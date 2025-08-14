package read

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
)

func (r *WeaviateReader) GetSchema(ctx context.Context, req mcp.CallToolRequest, args any) (*GetSchemaResp, error) {
	principal, err := r.PrincipalFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get principal: %w", err)
	}
	res, err := r.schemaReader.GetConsistentSchema(principal, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	return &GetSchemaResp{Schema: res.Objects}, nil
}
