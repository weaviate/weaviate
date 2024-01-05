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
	"strings"
	"sync"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

// AddReferences Class Instances in batch to the connected DB
func (b *BatchManager) AddReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference, repl *additional.ReplicationProperties,
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

	return b.addReferences(ctx, principal, refs, repl)
}

func (b *BatchManager) addReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference, repl *additional.ReplicationProperties,
) (BatchReferences, error) {
	if err := b.validateReferenceForm(refs); err != nil {
		return nil, NewErrInvalidUserInput("invalid params: %v", err)
	}

	batchReferences := b.validateReferencesConcurrently(ctx, principal, refs)

	if err := b.autodetectToClass(ctx, principal, batchReferences); err != nil {
		return nil, err
	}

	if res, err := b.vectorRepo.AddBatchReferences(ctx, batchReferences, repl); err != nil {
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
	principal *models.Principal, refs []*models.BatchReference,
) BatchReferences {
	c := make(chan BatchReference, len(refs))
	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, ref := range refs {
		wg.Add(1)
		go b.validateReference(ctx, principal, wg, ref, i, &c)
	}

	wg.Wait()
	close(c)

	return referencesChanToSlice(c)
}

// autodetectToClass gets the class name of the referenced class through the schema definition
func (b *BatchManager) autodetectToClass(ctx context.Context,
	principal *models.Principal, batchReferences BatchReferences,
) error {
	classPropTarget := make(map[string]string)
	scheme, err := b.schemaManager.GetSchema(principal)
	if err != nil {
		return NewErrInvalidUserInput("get schema: %v", err)
	}
	for i, ref := range batchReferences {
		// get to class from property datatype
		if ref.To.Class != "" || ref.Err != nil {
			continue
		}
		className := string(ref.From.Class)
		propName := schema.LowercaseFirstLetter(string(ref.From.Property))

		target, ok := classPropTarget[className+propName]
		if !ok {
			class := scheme.FindClassByName(ref.From.Class)
			if class == nil {
				batchReferences[i].Err = fmt.Errorf("class %s does not exist", className)
				continue
			}

			prop, err := schema.GetPropertyByName(class, propName)
			if err != nil {
				batchReferences[i].Err = fmt.Errorf("property %s does not exist for class %s", propName, className)
				continue
			}
			if len(prop.DataType) > 1 {
				continue // can't auto-detect for multi-target
			}
			target = prop.DataType[0] // datatype is the name of the class that is referenced
			classPropTarget[className+propName] = target
		}
		batchReferences[i].To.Class = target
	}
	return nil
}

func (b *BatchManager) validateReference(ctx context.Context, principal *models.Principal,
	wg *sync.WaitGroup, ref *models.BatchReference, i int, resultsC *chan BatchReference,
) {
	defer wg.Done()
	var validateErrors []error
	source, err := crossref.ParseSource(string(ref.From))
	if err != nil {
		validateErrors = append(validateErrors, err)
	} else if !source.Local {
		validateErrors = append(validateErrors, fmt.Errorf("source class must always point to the local peer, but got %s",
			source.PeerName))
	}

	target, err := crossref.Parse(string(ref.To))
	if err != nil {
		validateErrors = append(validateErrors, err)
	} else if !target.Local {
		validateErrors = append(validateErrors, fmt.Errorf("importing network references in batch is not possible. "+
			"Please perform a regular non-batch import for network references, got peer %s",
			target.PeerName))
	}

	if len(validateErrors) == 0 {
		err = nil
	} else {
		err = joinErrors(validateErrors)
	}

	if err == nil && shouldValidateMultiTenantRef(ref.Tenant, source, target) {
		// can only validate multi-tenancy when everything above succeeds
		err = validateReferenceMultiTenancy(ctx, principal,
			b.schemaManager, b.vectorRepo, source, target, ref.Tenant)
	}

	*resultsC <- BatchReference{
		From:          source,
		To:            target,
		Err:           err,
		OriginalIndex: i,
		Tenant:        ref.Tenant,
	}
}

func validateReferenceMultiTenancy(ctx context.Context,
	principal *models.Principal, schemaManager schemaManager,
	repo VectorRepo, source *crossref.RefSource, target *crossref.Ref,
	tenant string,
) error {
	if source == nil || target == nil {
		return fmt.Errorf("can't validate multi-tenancy for nil refs")
	}

	sourceClass, targetClass, err := getReferenceClasses(
		ctx, principal, schemaManager, source.Class.String(), target.Class)
	if err != nil {
		return err
	}

	sourceEnabled := schema.MultiTenancyEnabled(sourceClass)
	targetEnabled := schema.MultiTenancyEnabled(targetClass)

	if !sourceEnabled && targetEnabled {
		return fmt.Errorf("invalid reference: cannot reference a multi-tenant " +
			"enabled class from a non multi-tenant enabled class")
	}
	if sourceEnabled && !targetEnabled {
		if err := validateTenantRefObject(ctx, repo, sourceClass, source.TargetID, tenant); err != nil {
			return fmt.Errorf("source: %w", err)
		}
		if err := validateTenantRefObject(ctx, repo, targetClass, target.TargetID, ""); err != nil {
			return fmt.Errorf("target: %w", err)
		}
	}
	// if both classes have MT enabled but different tenant keys,
	// no cross-tenant references can be made
	if sourceEnabled && targetEnabled {
		if err := validateTenantRefObject(ctx, repo, sourceClass, source.TargetID, tenant); err != nil {
			return fmt.Errorf("source: %w", err)
		}
		if err := validateTenantRefObject(ctx, repo, targetClass, target.TargetID, tenant); err != nil {
			return fmt.Errorf("target: %w", err)
		}
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

// validateTenantRefObject ensures that object exist for the given tenant key.
// This asserts that no cross-tenant references can occur,
// as a class+id which belongs to a different
// tenant will not be found in the searched tenant shard
func validateTenantRefObject(ctx context.Context, repo VectorRepo,
	class *models.Class, ID strfmt.UUID, tenant string,
) error {
	exists, err := repo.Exists(ctx, class.Class, ID, nil, tenant)
	if err != nil {
		return fmt.Errorf("get object %s/%s: %w", class.Class, ID, err)
	}
	if !exists {
		return fmt.Errorf("object %s/%s not found for tenant %q", class.Class, ID, tenant)
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
