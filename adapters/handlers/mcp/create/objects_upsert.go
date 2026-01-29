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

package create

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (c *WeaviateCreator) UpsertObject(ctx context.Context, req mcp.CallToolRequest, args UpsertObjectArgs) (*UpsertObjectResp, error) {
	// Authorize for CREATE and UPDATE operations
	principal, err := c.Authorize(ctx, req, authorization.CREATE)
	if err != nil {
		return nil, err
	}

	if _, err := c.Authorize(ctx, req, authorization.UPDATE); err != nil {
		return nil, err
	}

	// Validate that we have at least one object
	if len(args.Objects) == 0 {
		return nil, fmt.Errorf("at least one object is required")
	}

	// Convert input objects to models.Object
	modelObjects := make([]*models.Object, len(args.Objects))
	for i, obj := range args.Objects {
		// Convert vectors map to models.Vectors
		var vectors models.Vectors
		if len(obj.Vectors) > 0 {
			vectors = make(models.Vectors)
			for name, vec := range obj.Vectors {
				vectors[name] = vec
			}
		}

		// Build the object
		modelObj := &models.Object{
			Class:      args.CollectionName,
			Tenant:     args.TenantName,
			Properties: obj.Properties,
			Vectors:    vectors,
		}

		// Set UUID if provided
		if obj.UUID != "" {
			if !strfmt.IsUUID(obj.UUID) {
				return nil, fmt.Errorf("invalid UUID for object")
			}
			modelObj.ID = strfmt.UUID(obj.UUID)
		}

		modelObjects[i] = modelObj
	}

	// Call batch add operation
	batchResults, err := c.batchManager.AddObjects(ctx, principal, modelObjects, nil, nil)
	if err != nil {
		// If the batch operation itself fails (e.g., collection doesn't exist),
		// return error details for all objects in the results
		results := make([]UpsertObjectResult, len(args.Objects))
		for i := range results {
			results[i] = UpsertObjectResult{
				Error: err.Error(),
			}
		}
		return &UpsertObjectResp{Results: results}, nil
	}

	// Convert batch results to response
	results := make([]UpsertObjectResult, len(batchResults))

	for i, result := range batchResults {
		if result.Err != nil {
			results[i] = UpsertObjectResult{
				Error: result.Err.Error(),
			}
		} else {
			results[i] = UpsertObjectResult{
				ID: result.UUID.String(),
			}
		}
	}

	return &UpsertObjectResp{Results: results}, nil
}
