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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (r *WeaviateReader) GetCollectionConfig(ctx context.Context, req mcp.CallToolRequest, args GetCollectionConfigArgs) (*GetCollectionConfigResp, error) {
	// Authorize the request
	principal, err := r.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}
	res, err := r.schemaReader.GetConsistentSchema(ctx, principal, true)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// If collection_name is specified, filter to just that collection
	if args.CollectionName != "" {
		for _, class := range res.Objects.Classes {
			if class.Class == args.CollectionName {
				return &GetCollectionConfigResp{Collections: []*models.Class{class}}, nil
			}
		}
		return nil, fmt.Errorf("collection %q not found", args.CollectionName)
	}

	// Return all collections
	return &GetCollectionConfigResp{Collections: res.Objects.Classes}, nil
}
