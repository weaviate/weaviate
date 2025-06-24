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
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/handlers/rest/filterext"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// DeleteObjects deletes objects in batch based on the match filter
func (b *BatchManager) DeleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, deletionTimeUnixMilli *int64, dryRun *bool, output *string,
	repl *additional.ReplicationProperties, tenant string,
) (*BatchDeleteResponse, error) {
	class := "*"
	if match != nil {
		class = match.Class
	}

	err := b.authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.ShardsData(class, tenant)...)
	if err != nil {
		return nil, err
	}

	ctx = classcache.ContextWithClassCache(ctx)

	b.metrics.BatchDeleteInc()
	defer b.metrics.BatchDeleteDec()

	return b.deleteObjects(ctx, principal, match, deletionTimeUnixMilli, dryRun, output, repl, tenant)
}

// DeleteObjectsFromGRPCAfterAuth deletes objects in batch based on the match filter
func (b *BatchManager) DeleteObjectsFromGRPCAfterAuth(ctx context.Context, principal *models.Principal,
	params BatchDeleteParams,
	repl *additional.ReplicationProperties, tenant string,
) (BatchDeleteResult, error) {
	b.metrics.BatchDeleteInc()
	defer b.metrics.BatchDeleteDec()

	deletionTime := time.UnixMilli(b.timeSource.Now())
	return b.vectorRepo.BatchDeleteObjects(ctx, params, deletionTime, repl, tenant, 0)
}

func (b *BatchManager) deleteObjects(ctx context.Context, principal *models.Principal,
	match *models.BatchDeleteMatch, deletionTimeUnixMilli *int64, dryRun *bool, output *string,
	repl *additional.ReplicationProperties, tenant string,
) (*BatchDeleteResponse, error) {
	params, schemaVersion, err := b.validateBatchDelete(ctx, principal, match, dryRun, output)
	if err != nil {
		return nil, errors.Wrap(err, "validate")
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := b.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
	}
	var deletionTime time.Time
	if deletionTimeUnixMilli != nil {
		deletionTime = time.UnixMilli(*deletionTimeUnixMilli)
	}

	result, err := b.vectorRepo.BatchDeleteObjects(ctx, *params, deletionTime, repl, tenant, schemaVersion)
	if err != nil {
		return nil, fmt.Errorf("batch delete objects: %w", err)
	}

	return b.toResponse(match, params.Output, result)
}

func (b *BatchManager) toResponse(match *models.BatchDeleteMatch, output string,
	result BatchDeleteResult,
) (*BatchDeleteResponse, error) {
	response := &BatchDeleteResponse{
		Match:        match,
		Output:       output,
		DeletionTime: result.DeletionTime,
		DryRun:       result.DryRun,
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
) (*BatchDeleteParams, uint64, error) {
	if match == nil {
		return nil, 0, errors.New("empty match clause")
	}

	if len(match.Class) == 0 {
		return nil, 0, errors.New("empty match.class clause")
	}

	if match.Where == nil {
		return nil, 0, errors.New("empty match.where clause")
	}

	// Validate schema given in body with the weaviate schema
	vclasses, err := b.schemaManager.GetCachedClass(ctx, principal, match.Class)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get class: %s: %w", match.Class, err)
	}
	if vclasses[match.Class].Class == nil {
		return nil, 0, fmt.Errorf("failed to get class: %s", match.Class)
	}
	class := vclasses[match.Class].Class

	filter, err := filterext.Parse(match.Where, class.Class)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse where filter: %w", err)
	}

	err = filters.ValidateFilters(b.classGetterFunc(ctx, principal), filter)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid where filter: %w", err)
	}

	dryRunParam := false
	if dryRun != nil {
		dryRunParam = *dryRun
	}

	outputParam, err := verbosity.ParseOutput(output)
	if err != nil {
		return nil, 0, err
	}

	params := &BatchDeleteParams{
		ClassName: schema.ClassName(class.Class),
		Filters:   filter,
		DryRun:    dryRunParam,
		Output:    outputParam,
	}
	return params, vclasses[match.Class].Version, nil
}

func (b *BatchManager) classGetterFunc(ctx context.Context, principal *models.Principal) func(string) (*models.Class, error) {
	return func(name string) (*models.Class, error) {
		if err := b.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Collections(name)...); err != nil {
			return nil, err
		}
		class := b.schemaManager.ReadOnlyClass(name)
		if class == nil {
			return nil, fmt.Errorf("could not find class %s in schema", name)
		}
		return class, nil
	}
}
