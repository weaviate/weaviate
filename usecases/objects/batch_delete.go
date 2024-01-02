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

package objects

import (
	"context"
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
)

// DeleteObjects deletes objects in batch based on the match filter
func (b *BatchManager) DeleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, dryRun *bool, output *string,
	repl *additional.ReplicationProperties, tenant string,
) (*BatchDeleteResponse, error) {
	err := b.authorizer.Authorize(principal, "delete", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	b.metrics.BatchDeleteInc()
	defer b.metrics.BatchDeleteDec()

	return b.deleteObjects(ctx, principal, match, dryRun, output, repl, tenant)
}

func (b *BatchManager) deleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, dryRun *bool, output *string,
	repl *additional.ReplicationProperties, tenant string,
) (*BatchDeleteResponse, error) {
	params, err := b.validateBatchDelete(ctx, principal, match, dryRun, output)
	if err != nil {
		return nil, NewErrInvalidUserInput("validate: %v", err)
	}

	result, err := b.vectorRepo.BatchDeleteObjects(ctx, *params, repl, tenant)
	if err != nil {
		return nil, fmt.Errorf("batch delete objects: %w", err)
	}

	return b.toResponse(match, params.Output, result)
}

func (b *BatchManager) toResponse(match *models.BatchDeleteMatch, output string,
	result BatchDeleteResult,
) (*BatchDeleteResponse, error) {
	response := &BatchDeleteResponse{
		Match:  match,
		Output: output,
		DryRun: result.DryRun,
		Result: BatchDeleteResult{
			Matches: result.Matches,
			Limit:   result.Limit,
			Objects: result.Objects,
		},
	}
	return response, nil
}

func (b *BatchManager) validateBatchDelete(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, dryRun *bool, output *string,
) (*BatchDeleteParams, error) {
	if match == nil {
		return nil, errors.New("empty match clause")
	}

	if len(match.Class) == 0 {
		return nil, errors.New("empty match.class clause")
	}

	if match.Where == nil {
		return nil, errors.New("empty match.where clause")
	}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %s", err)
	}

	class := s.FindClassByName(schema.ClassName(match.Class))
	if class == nil {
		return nil, fmt.Errorf("class: %v doesn't exist", match.Class)
	}

	filter, err := filterext.Parse(match.Where, class.Class)
	if err != nil {
		return nil, fmt.Errorf("failed to parse where filter: %s", err)
	}

	err = filters.ValidateFilters(s, filter)
	if err != nil {
		return nil, fmt.Errorf("invalid where filter: %s", err)
	}

	dryRunParam := false
	if dryRun != nil {
		dryRunParam = *dryRun
	}

	outputParam, err := verbosity.ParseOutput(output)
	if err != nil {
		return nil, err
	}

	params := &BatchDeleteParams{
		ClassName: schema.ClassName(class.Class),
		Filters:   filter,
		DryRun:    dryRunParam,
		Output:    outputParam,
	}
	return params, nil
}
