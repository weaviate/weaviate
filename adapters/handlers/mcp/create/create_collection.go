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
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// CreateCollection creates a new collection (class) in Weaviate
func (c *WeaviateCreator) CreateCollection(ctx context.Context, req mcp.CallToolRequest, args CreateCollectionArgs) (*CreateCollectionResp, error) {
	// Authorize the request
	principal, err := c.Authorize(ctx, req, authorization.CREATE)
	if err != nil {
		return nil, err
	}

	// Build the class object
	class := &models.Class{
		Class:       args.CollectionName,
		Description: args.Description,
	}

	// Convert properties from []any to []*models.Property
	if len(args.Properties) > 0 {
		propertiesJSON, err := json.Marshal(args.Properties)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal properties: %w", err)
		}

		var properties []*models.Property
		if err := json.Unmarshal(propertiesJSON, &properties); err != nil {
			return nil, fmt.Errorf("failed to unmarshal properties: %w", err)
		}
		class.Properties = properties
	}

	// Set inverted index config if provided
	if args.InvertedIndexConfig != nil {
		invertedIndexConfigJSON, err := json.Marshal(args.InvertedIndexConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal invertedIndexConfig: %w", err)
		}

		var invertedIndexConfig models.InvertedIndexConfig
		if err := json.Unmarshal(invertedIndexConfigJSON, &invertedIndexConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal invertedIndexConfig: %w", err)
		}
		class.InvertedIndexConfig = &invertedIndexConfig
	}

	// Set vector config if provided
	if args.VectorConfig != nil {
		vectorConfigJSON, err := json.Marshal(args.VectorConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal vectorConfig: %w", err)
		}

		var vectorConfig map[string]models.VectorConfig
		if err := json.Unmarshal(vectorConfigJSON, &vectorConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal vectorConfig: %w", err)
		}
		class.VectorConfig = vectorConfig
	}

	// Set multi-tenancy config if provided
	if args.MultiTenancyConfig != nil {
		multiTenancyConfigJSON, err := json.Marshal(args.MultiTenancyConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal multiTenancyConfig: %w", err)
		}

		var multiTenancyConfig models.MultiTenancyConfig
		if err := json.Unmarshal(multiTenancyConfigJSON, &multiTenancyConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal multiTenancyConfig: %w", err)
		}
		class.MultiTenancyConfig = &multiTenancyConfig
	}

	// Create the collection
	createdClass, _, err := c.schemaManager.AddClass(ctx, principal, class)
	if err != nil {
		return nil, fmt.Errorf("failed to create collection: %w", err)
	}

	return &CreateCollectionResp{
		CollectionName: createdClass.Class,
	}, nil
}
