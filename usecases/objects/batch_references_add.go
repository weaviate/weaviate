//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

// AddReferences Class Instances in batch to the connected DB
func (b *BatchManager) AddReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference, repl *additional.ReplicationProperties, tenantKey string,
) (BatchReferences, error) {
	err := b.authorizer.Authorize(principal, "update", "batch/*")
	if err != nil {
		return nil, err
	}

	unlock, err := b.locks.LockSchema()
	if err != nil {
		return nil, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	b.metrics.BatchRefInc()
	defer b.metrics.BatchRefDec()

	return b.addReferences(ctx, principal, refs, repl, tenantKey)
}

func (b *BatchManager) addReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference, repl *additional.ReplicationProperties,
	tenantKey string,
) (BatchReferences, error) {
	if err := b.validateReferenceForm(refs); err != nil {
		return nil, NewErrInvalidUserInput("invalid params: %v", err)
	}

	batchReferences := b.validateReferencesConcurrently(ctx, principal, refs, tenantKey)
	if res, err := b.vectorRepo.AddBatchReferences(ctx, batchReferences, repl, tenantKey); err != nil {
		return nil, NewErrInternal("could not add batch request to connector: %v", err)
	} else {
		return res, nil
	}
}

func (b *BatchManager) validateReferenceForm(refs []*models.BatchReference) error {
	if len(refs) == 0 {
		return fmt.Errorf("length cannot be 0, need at least one reference for batching")
	}

	return nil
}

func (b *BatchManager) validateReferencesConcurrently(ctx context.Context,
	principal *models.Principal, refs []*models.BatchReference, tenantKey string,
) BatchReferences {
	c := make(chan BatchReference, len(refs))
	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, ref := range refs {
		wg.Add(1)
		go b.validateReference(ctx, principal, wg, ref, i, &c, tenantKey)
	}

	wg.Wait()
	close(c)
	return referencesChanToSlice(c)
}

func (b *BatchManager) validateReference(ctx context.Context, principal *models.Principal,
	wg *sync.WaitGroup, ref *models.BatchReference, i int, resultsC *chan BatchReference,
	tenantKey string,
) {
	defer wg.Done()
	var errors []error
	source, err := crossref.ParseSource(string(ref.From))
	if err != nil {
		errors = append(errors, err)
	} else if !source.Local {
		errors = append(errors, fmt.Errorf("source class must always point to the local peer, but got %s",
			source.PeerName))
	}

	target, err := crossref.Parse(string(ref.To))
	if err != nil {
		errors = append(errors, err)
	} else if !target.Local {
		errors = append(errors, fmt.Errorf("importing network references in batch is not possible. "+
			"Please perform a regular non-batch import for network references, got peer %s",
			target.PeerName))
	}

	if len(errors) == 0 {
		err = nil
	} else {
		err = joinErrors(errors)
	}

	if shouldValidateMultiTenantRef(tenantKey, source, target) && err == nil {
		// can only validate multi-tenancy when everything above succeeds
		err = validateReferenceMultiTenancy(ctx, principal,
			b.schemaManager, b.vectorRepo, source, target, tenantKey)
	}

	*resultsC <- BatchReference{
		From:          source,
		To:            target,
		Err:           err,
		OriginalIndex: i,
	}
}

func validateReferenceMultiTenancy(ctx context.Context,
	principal *models.Principal, schemaManager schemaManager,
	repo VectorRepo, source *crossref.RefSource, target *crossref.Ref,
	tenantKey string,
) error {
	if source == nil || target == nil {
		return fmt.Errorf("can't validate multi-tenancy for nil refs")
	}

	sourceClass, targetClass, err := getReferenceClasses(
		ctx, principal, schemaManager, source.Class.String(), target.Class)
	if err != nil {
		return err
	}

	// if both classes have MT enabled but different tenant keys,
	// no cross-tenant references can be made
	if sourceClass.MultiTenancyConfig != nil && targetClass.MultiTenancyConfig != nil &&
		sourceClass.MultiTenancyConfig.Enabled && targetClass.MultiTenancyConfig.Enabled {
		if sourceClass.MultiTenancyConfig.TenantKey != targetClass.MultiTenancyConfig.TenantKey {
			return fmt.Errorf("invalid reference: source class %q tenant key %q "+
				"is different than target class %q tenant key %q",
				sourceClass.Class, sourceClass.MultiTenancyConfig.TenantKey,
				targetClass.Class, targetClass.MultiTenancyConfig.TenantKey)
		}

		err = validateTenantRefObjects(ctx, repo,
			sourceClass, targetClass, source, target, tenantKey)
		if err != nil {
			return err
		}
	}

	// the other way around is ok, a MT-enabled class may reference a
	// non-MT-enabled class.
	if sourceClass.MultiTenancyConfig == nil && targetClass.MultiTenancyConfig != nil {
		return fmt.Errorf("invalid reference: cannot reference a multi-tenant " +
			"enabled class from a non multi-tenant enabled class")
	}

	return nil
}

func getReferenceClasses(ctx context.Context,
	principal *models.Principal, schemaManager schemaManager,
	classFrom, classTo string,
) (sourceClass *models.Class, targetClass *models.Class, err error) {
	if classFrom == "" || classTo == "" {
		err = fmt.Errorf("references involving a multi-tenancy enabled class " +
			"requires class name in the beacon url")
		return
	}

	sourceClass, err = schemaManager.GetClass(ctx, principal, classFrom)
	if err != nil {
		err = fmt.Errorf("get source class %q: %w", classFrom, err)
		return
	}
	if sourceClass == nil {
		err = fmt.Errorf("source class %q not found in schema", classFrom)
		return
	}

	targetClass, err = schemaManager.GetClass(ctx, principal, classTo)
	if err != nil {
		err = fmt.Errorf("get target class %q: %w", classTo, err)
		return
	}
	if targetClass == nil {
		err = fmt.Errorf("target class %q not found in schema", classTo)
		return
	}
	return
}

// validateTenantRefObjects ensures that both source and target objects
// exist for the given tenant key. This asserts that no cross-tenant
// references can occur, as a class+id which belongs to a different
// tenant will not be found in the searched tenant shard
func validateTenantRefObjects(ctx context.Context, repo VectorRepo,
	sourceClass, targetClass *models.Class, source *crossref.RefSource,
	target *crossref.Ref, tenantKey string,
) error {
	exists, err := repo.Exists(ctx, sourceClass.Class,
		source.TargetID, nil, tenantKey)
	if err != nil {
		return fmt.Errorf("get source object %s/%s: %w", sourceClass.Class, source.TargetID, err)
	}
	if !exists {
		return fmt.Errorf("source object %s/%s not found for tenant %q",
			source.Class, source.TargetID, tenantKey)
	}

	exists, err = repo.Exists(ctx, targetClass.Class,
		target.TargetID, nil, tenantKey)
	if err != nil {
		return fmt.Errorf("get target object %s/%s: %w", targetClass.Class, target.TargetID, err)
	}
	if !exists {
		return fmt.Errorf("target object %s/%s not found for tenant %q",
			target.Class, target.TargetID, tenantKey)
	}

	return nil
}

func referencesChanToSlice(c chan BatchReference) BatchReferences {
	result := make([]BatchReference, len(c))
	for reference := range c {
		result[reference.OriginalIndex] = reference
	}

	return result
}

func joinErrors(errors []error) error {
	errorStrings := []string{}
	for _, err := range errors {
		if err != nil {
			errorStrings = append(errorStrings, err.Error())
		}
	}

	if len(errorStrings) == 0 {
		return nil
	}

	return fmt.Errorf(strings.Join(errorStrings, ", "))
}
