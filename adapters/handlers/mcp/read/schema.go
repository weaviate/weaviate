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

package read

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (r *WeaviateReader) GetSchema(ctx context.Context, req mcp.CallToolRequest, args any) (*GetSchemaResp, error) {
	principal, err := r.Authorize(req, authorization.READ)
	if err != nil {
		return nil, err
	}
	res, err := r.schemaReader.GetConsistentSchema(principal, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}
	return &GetSchemaResp{Schema: res.Objects}, nil
}
