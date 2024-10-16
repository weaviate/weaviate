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

	"github.com/weaviate/weaviate/entities/classcache"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization"

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
	err := b.authorizer.Authorize(principal, authorization.UPDATE, authorization.ALL_BATCH)
	if err != nil {
		return nil, err
	}

	ctx = classcache.ContextWithClassCache(ctx)

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

	// MT validation must be done after auto-detection as we cannot know the target class beforehand in all cases
	var schemaVersion uint64
	for i, ref := range batchReferences {
		if ref.Err == nil {
			if shouldValidateMultiTenantRef(ref.Tenant, ref.From, ref.To) {
				// can only validate multi-tenancy when everything above succeeds
				classVersion, err := validateReferenceMultiTenancy(ctx, principal, b.schemaManager, b.vectorRepo, ref.From, ref.To, ref.Tenant)
				if err != nil {
					batchReferences[i].Err = err
				}
				if classVersion > schemaVersion {
					schemaVersion = classVersion
				}
			}
		}
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := b.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
	}
	if res, err := b.vectorRepo.AddBatchReferences(ctx, batchReferences, repl, schemaVersion); err != nil {
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
		i := i
		ref := ref
		wg.Add(1)
		enterrors.GoWrapper(func() { b.validateReference(ctx, principal, wg, ref, i, &c) }, b.logger)
	}

	wg.Wait()
	close(c)

	return referencesChanToSlice(c)
}

// autodetectToClass gets the class name of the referenced class through the schema definition
func (b *BatchManager) autodetectToClass(ctx context.Context,
	principal *models.Principal, batchReferences BatchReferences,
) error {
	classPropTarget := make(map[string]string, len(batchReferences))
	for i, ref := range batchReferences {
		// get to class from property datatype
		if ref.To.Class != "" || ref.Err != nil {
			continue
		}
		className := string(ref.From.Class)
		propName := schema.LowercaseFirstLetter(string(ref.From.Property))

		target, ok := classPropTarget[className+propName]
		if !ok {
			class, err := b.schemaManager.GetClass(ctx, principal, ref.From.Class.String())
			if class == nil || err != nil {
				batchReferences[i].Err = fmt.Errorf("class %s does not exist or there was an error getting it err=%v", className, err)
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

	// target id must be lowercase
	target.TargetID = strfmt.UUID(strings.ToLower(target.TargetID.String()))

	if len(validateErrors) == 0 {
		err = nil
	} else {
		err = joinErrors(validateErrors)
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
) (uint64, error) {
	if source == nil || target == nil {
		return 0, fmt.Errorf("can't validate multi-tenancy for nil refs")
	}

	sourceClass, targetClass, schemaVersion, err := getReferenceClasses(
		ctx, principal, schemaManager, source.Class.String(), source.Property.String(), target.Class)
	if err != nil {
		return 0, err
	}

	sourceEnabled := schema.MultiTenancyEnabled(sourceClass)
	targetEnabled := schema.MultiTenancyEnabled(targetClass)

	if !sourceEnabled && targetEnabled {
		return 0, fmt.Errorf("invalid reference: cannot reference a multi-tenant " +
			"enabled class from a non multi-tenant enabled class")
	}
	if sourceEnabled && !targetEnabled {
		if err := validateTenantRefObject(ctx, repo, sourceClass, source.TargetID, tenant); err != nil {
			return 0, fmt.Errorf("source: %w", err)
		}
		if err := validateTenantRefObject(ctx, repo, targetClass, target.TargetID, ""); err != nil {
			return 0, fmt.Errorf("target: %w", err)
		}
	}
	// if both classes have MT enabled but different tenant keys,
	// no cross-tenant references can be made
	if sourceEnabled && targetEnabled {
		if err := validateTenantRefObject(ctx, repo, sourceClass, source.TargetID, tenant); err != nil {
			return 0, fmt.Errorf("source: %w", err)
		}
		if err := validateTenantRefObject(ctx, repo, targetClass, target.TargetID, tenant); err != nil {
			return 0, fmt.Errorf("target: %w", err)
		}
	}

	return schemaVersion, nil
}

func getReferenceClasses(ctx context.Context,
	principal *models.Principal, schemaManager schemaManager,
	classFrom, fromProperty, classTo string,
) (sourceClass *models.Class, targetClass *models.Class, schemaVersion uint64, err error) {
	if classFrom == "" {
		err = fmt.Errorf("references involving a multi-tenancy enabled class " +
			"requires class name in the source beacon url")
		return
	}

	vclasses, err := schemaManager.GetCachedClass(ctx, principal, classFrom)
	if err != nil {
		err = fmt.Errorf("get source class %q: %w", classFrom, err)
		return
	}
	if vclasses[classFrom].Class == nil {
		err = fmt.Errorf("source class %q not found in schema", classFrom)
		return
	}

	sourceClass = vclasses[classFrom].Class
	schemaVersion = vclasses[classFrom].Version

	// we can auto-detect the to class from the schema if it is a single target reference
	if classTo == "" {
		refProp, err2 := schema.GetPropertyByName(sourceClass, fromProperty)
		if err2 != nil {
			err = fmt.Errorf("get source refprop %q: %w", classFrom, err2)
			return
		}

		if len(refProp.DataType) != 1 {
			err = fmt.Errorf("multi-target references require the class name in the target beacon url")
			return
		}
		classTo = refProp.DataType[0]
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
