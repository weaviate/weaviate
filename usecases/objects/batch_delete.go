//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/filterext"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

const (
	OutputMinimal = "minimal"
	OutputVerbose = "verbose"
)

// DeleteObjects deletes objects in batch based on the match filter
func (b *BatchManager) DeleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, dryRun *bool, output *string) (*BatchDeleteResponse, error) {
	err := b.authorizer.Authorize(principal, "delete", "batch/objects")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockConnector()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return b.deleteObjects(ctx, principal, match, dryRun, output)
}

func (b *BatchManager) deleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, dryRun *bool, output *string) (*BatchDeleteResponse, error) {
	params, err := b.validateBatchDelete(ctx, principal, match, dryRun, output)
	if err != nil {
		return nil, NewErrInvalidUserInput("validate: %v", err)
	}

	result, err := b.vectorRepo.BatchDeleteObjects(ctx, *params)
	if err != nil {
		return nil, NewErrInternal("batch delete objects: %#v", err)
	}

	return b.toResponse(match, params.Output, result)
}

func (b *BatchManager) toResponse(match *models.BatchDeleteMatch, output string,
	result BatchDeleteResult) (*BatchDeleteResponse, error) {
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
	match *models.BatchDeleteMatch, dryRun *bool, output *string) (*BatchDeleteParams, error) {
	if match == nil {
		return nil, errors.New("empty match clause")
	}

	if len(match.Class) == 0 {
		return nil, errors.New("empty match.class clause")
	}

	if match.Where == nil {
		return nil, errors.New("empty match.where clause")
	}

	ec := &errorCompounder{}

	// Validate schema given in body with the weaviate schema
	s, err := b.schemaManager.GetSchema(principal)
	ec.add(err)

	class := s.FindClassByName(schema.ClassName(match.Class))
	if class == nil {
		ec.add(errors.Errorf("class: %v doesn't exist", match.Class))
	}

	filter, err := filterext.Parse(match.Where)
	if err != nil {
		ec.add(errors.Wrap(err, "invalid where"))
	}

	if class != nil {
		err = filters.ValidateFiltersWithClassName(s, class.Class, filter)
		if err != nil {
			ec.add(errors.Wrap(err, "invalid where"))
		}
	}

	dryRunParam := false
	if dryRun != nil {
		dryRunParam = *dryRun
	}

	outputParam := OutputMinimal
	if output != nil {
		switch *output {
		case OutputMinimal, OutputVerbose:
			outputParam = *output
		default:
			ec.add(errors.Errorf(`invalid output: "%s", possible values are: "%s", "%s"`,
				*output, OutputMinimal, OutputVerbose))
		}
	}

	if ec.toError() != nil {
		return nil, ec.toError()
	}

	params := &BatchDeleteParams{
		ClassName: schema.ClassName(class.Class),
		Filters:   filter,
		DryRun:    dryRunParam,
		Output:    outputParam,
	}
	return params, nil
}
