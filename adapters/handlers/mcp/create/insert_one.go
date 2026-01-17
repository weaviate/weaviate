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

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (c *WeaviateCreator) UpsertObject(ctx context.Context, req mcp.CallToolRequest, args UpsertObjectArgs) (*UpsertObjectResp, error) {
	// Authorize for CREATE operation (we'll handle UPDATE authorization in the update path)
	principal, err := c.Authorize(ctx, req, authorization.CREATE)
	if err != nil {
		return nil, err
	}

	// Convert vectors map to models.Vectors
	var vectors models.Vectors
	if len(args.Vectors) > 0 {
		vectors = make(models.Vectors)
		for name, vec := range args.Vectors {
			vectors[name] = vec
		}
	}

	// Build the object
	obj := &models.Object{
		Class:      args.CollectionName,
		Tenant:     args.TenantName,
		Properties: args.Properties,
		Vectors:    vectors,
	}

	var result *models.Object

	// If UUID is provided, check if object exists and update or insert accordingly
	if args.UUID != "" {
		uuid := strfmt.UUID(args.UUID)
		obj.ID = uuid

		// Try to get the existing object
		existing, err := c.objectsManager.GetObject(ctx, principal, args.CollectionName, uuid, additional.Properties{}, nil, args.TenantName)

		if err == nil && existing != nil {
			// Object exists, perform update
			// Re-authorize for UPDATE
			if _, err := c.Authorize(ctx, req, authorization.UPDATE); err != nil {
				return nil, err
			}

			result, err = c.objectsManager.UpdateObject(ctx, principal, args.CollectionName, uuid, obj, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to update object: %w", err)
			}
		} else {
			// Object doesn't exist, create new with provided UUID
			result, err = c.objectsManager.AddObject(ctx, principal, obj, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create object with UUID: %w", err)
			}
		}
	} else {
		// No UUID provided, create new object with auto-generated UUID
		result, err = c.objectsManager.AddObject(ctx, principal, obj, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create object: %w", err)
		}
	}

	return &UpsertObjectResp{ID: result.ID.String()}, nil
}
