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
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (c *WeaviateCreator) CreateCollection(ctx context.Context, req mcp.CallToolRequest, args CreateCollectionArgs) (resp *CreateCollectionResp, retErr error) {
	if err := c.requireWriteAccess(); err != nil {
		return nil, err
	}
	principal, err := c.Authorize(ctx, req, authorization.CREATE)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = namespacing.StripErrForPrincipal(principal, retErr) }()

	class, err := classFromArgs(args)
	if err != nil {
		return nil, err
	}
	stored, _, err := c.schemaManager.AddClass(ctx, principal, class)
	if err != nil {
		return nil, fmt.Errorf("failed to create collection: %w", err)
	}
	return &CreateCollectionResp{Collection: namespacing.StripClassResponse(principal, stored)}, nil
}

// classFromArgs decodes the arguments as a REST class body, so a mistyped key
// in a typed nested object is an error rather than silently dropped.
func classFromArgs(args CreateCollectionArgs) (*models.Class, error) {
	doc, err := json.Marshal(map[string]any{
		"class":               args.CollectionName,
		"description":         args.Description,
		"properties":          args.Properties,
		"vectorConfig":        args.VectorConfig,
		"moduleConfig":        args.ModuleConfig,
		"multiTenancyConfig":  args.MultiTenancyConfig,
		"invertedIndexConfig": args.InvertedIndexConfig,
		"replicationConfig":   args.ReplicationConfig,
		"objectTtlConfig":     args.ObjectTTLConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid collection config: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.DisallowUnknownFields()
	class := &models.Class{}
	if err := dec.Decode(class); err != nil {
		return nil, fmt.Errorf("invalid collection config: %w", err)
	}
	if err := class.Validate(strfmt.Default); err != nil {
		return nil, fmt.Errorf("invalid collection config: %w", err)
	}
	return class, nil
}
