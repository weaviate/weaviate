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

// DeleteObjects deletes all objects from a collection
func (c *WeaviateCreator) DeleteObjects(ctx context.Context, req mcp.CallToolRequest, args DeleteObjectsArgs) (*DeleteObjectsResp, error) {
	// Authorize the request
	principal, err := c.Authorize(ctx, req, authorization.DELETE)
	if err != nil {
		return nil, err
	}

	// Set default dry_run to true for safety
	dryRun := true
	if args.DryRun != nil {
		dryRun = *args.DryRun
	}

	// Create a match filter
	match := &models.BatchDeleteMatch{
		Class: args.CollectionName,
	}

	// Parse and add the where filter if provided, otherwise create a "match all" filter
	var whereFilter models.WhereFilter
	if args.Where != nil {
		whereJSON, err := json.Marshal(args.Where)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal where filter: %w", err)
		}

		if err := json.Unmarshal(whereJSON, &whereFilter); err != nil {
			return nil, fmt.Errorf("failed to unmarshal where filter: %w", err)
		}
	} else {
		// When no where filter is provided, create a filter that matches all objects
		// This uses the Like operator with wildcard "*" which matches everything
		// We use "_id" as the path since it's a metadata field that exists on all objects
		glob := "*"
		whereFilter = models.WhereFilter{
			Operator:  "Like",
			Path:      []string{"_id"},
			ValueText: &glob,
		}
	}
	match.Where = &whereFilter

	// Call the batch delete operation
	resp, err := c.batchManager.DeleteObjects(ctx, principal, match, nil, &dryRun, nil, nil, args.TenantName)
	if err != nil {
		return nil, fmt.Errorf("failed to delete objects: %w", err)
	}

	// Calculate the number of deleted objects
	// In dry run mode, deleted=0, otherwise deleted=matches
	deleted := 0
	matches := int(resp.Result.Matches)
	if !resp.DryRun {
		deleted = matches
	}

	return &DeleteObjectsResp{
		Deleted: deleted,
		Matches: matches,
		DryRun:  resp.DryRun,
	}, nil
}
