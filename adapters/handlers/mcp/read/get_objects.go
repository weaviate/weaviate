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

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/objects"
)

func (r *WeaviateReader) GetObjects(ctx context.Context, req mcp.CallToolRequest, args GetObjectsArgs) (*GetObjectsResp, error) {
	// Authorize the request
	principal, err := r.Authorize(ctx, req, authorization.READ)
	if err != nil {
		return nil, err
	}

	// Build additional properties based on requested metadata
	addl := r.buildAdditionalProperties(args)

	// If UUIDs are provided, fetch specific objects
	if len(args.UUIDs) > 0 {
		objects, err := r.getObjectsByUUIDs(ctx, principal, args, addl)
		if err != nil {
			return nil, err
		}
		return &GetObjectsResp{Objects: objects}, nil
	}

	// Otherwise, fetch a paginated list of objects
	objects, err := r.getObjectsList(ctx, principal, args, addl)
	if err != nil {
		return nil, err
	}

	return &GetObjectsResp{Objects: objects}, nil
}

func (r *WeaviateReader) getObjectsByUUIDs(ctx context.Context, principal *models.Principal,
	args GetObjectsArgs, addl additional.Properties,
) ([]*models.Object, error) {
	objects := make([]*models.Object, 0, len(args.UUIDs))

	for _, uuidStr := range args.UUIDs {
		uuid := strfmt.UUID(uuidStr)

		obj, err := r.objectsManager.GetObject(ctx, principal, args.CollectionName, uuid, addl, nil, args.TenantName)
		if err != nil {
			// If object not found, skip it (could also return error depending on requirements)
			continue
		}

		// Filter properties if specified
		if len(args.ReturnProperties) > 0 {
			obj = r.filterProperties(obj, args.ReturnProperties)
		}

		objects = append(objects, obj)
	}

	return objects, nil
}

func (r *WeaviateReader) getObjectsList(ctx context.Context, principal *models.Principal,
	args GetObjectsArgs, addl additional.Properties,
) ([]*models.Object, error) {
	if args.CollectionName == "" {
		return nil, fmt.Errorf("collection_name is required")
	}

	// Use Query method with QueryParams (same as REST endpoint does)
	var tenant *string
	if args.TenantName != "" {
		tenant = &args.TenantName
	}

	queryParams := &objects.QueryParams{
		Class:      args.CollectionName,
		Offset:     convertToInt64Ptr(args.Offset),
		Limit:      convertToInt64Ptr(args.Limit),
		Tenant:     tenant,
		Additional: addl,
	}

	objs, err := r.objectsManager.Query(ctx, principal, queryParams)
	if err != nil {
		return nil, fmt.Errorf("failed to get objects: %w", err)
	}

	// Filter properties if specified
	filteredObjects := make([]*models.Object, 0, len(objs))
	for _, obj := range objs {
		if len(args.ReturnProperties) > 0 {
			obj = r.filterProperties(obj, args.ReturnProperties)
		}
		filteredObjects = append(filteredObjects, obj)
	}

	return filteredObjects, nil
}

func convertToInt64Ptr(i *int) *int64 {
	if i == nil {
		return nil
	}
	val := int64(*i)
	return &val
}

func (r *WeaviateReader) buildAdditionalProperties(args GetObjectsArgs) additional.Properties {
	addl := additional.Properties{
		ID: true, // Always include ID
	}

	// Handle vector inclusion
	if args.IncludeVector {
		addl.Vector = true
	}

	// Handle metadata fields
	for _, field := range args.ReturnMetadata {
		switch field {
		case "id":
			addl.ID = true
		case "vector":
			addl.Vector = true
		case "creationTimeUnix":
			addl.CreationTimeUnix = true
		case "lastUpdateTimeUnix":
			addl.LastUpdateTimeUnix = true
		case "distance":
			addl.Distance = true
		case "score":
			addl.Score = true
		case "explainScore":
			addl.ExplainScore = true
		case "certainty":
			addl.Certainty = true
		}
	}

	return addl
}

func (r *WeaviateReader) filterProperties(obj *models.Object, properties []string) *models.Object {
	if obj == nil || obj.Properties == nil {
		return obj
	}

	// Create a map for quick lookup
	propsMap := make(map[string]bool)
	for _, prop := range properties {
		propsMap[prop] = true
	}

	// Filter properties
	filtered := make(map[string]interface{})
	if propsInterface, ok := obj.Properties.(map[string]interface{}); ok {
		for key, value := range propsInterface {
			if propsMap[key] {
				filtered[key] = value
			}
		}
		obj.Properties = filtered
	}

	return obj
}
