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

package create

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (c *WeaviateCreator) UpsertObject(ctx context.Context, req mcp.CallToolRequest, args UpsertObjectArgs) (*UpsertObjectResp, error) {
	principal, err := c.Authorize(ctx, req, authorization.CREATE)
	if err != nil {
		return nil, err
	}
	obj := models.Object{
		Class:      args.CollectionName,
		Tenant:     args.Tenant,
		Properties: args.Properties,
	}
	res, err := c.objectsManager.AddObject(ctx, principal, &obj, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to upsert object: %w", err)
	}
	return &UpsertObjectResp{ID: res.ID.String()}, nil
}
