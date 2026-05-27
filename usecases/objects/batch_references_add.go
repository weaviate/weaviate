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

package objects

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// AddReferences Class Instances in batch to the connected DB
func (b *BatchManager) AddReferences(ctx context.Context, principal *models.Principal,
	refs []*models.BatchReference, repl *additional.ReplicationProperties,
) (BatchReferences, error) {
	// only validates form of input, no schema access
	if err := validateReferenceForm(refs); err != nil {
		return nil, NewErrInvalidUserInput("invalid params: %v", err)
	}

	ctx = classcache.ContextWithClassCache(ctx)

	batchReferences := validateReferencesConcurrently(ctx, refs, b.logger)

	for idx := range batchReferences {
		if batchReferences[idx].Err != nil {
			continue
		}
		fromClass, _, err := b.resolveNS(principal, batchReferences[idx].From.Class.String())
		if err != nil {
			batchReferences[idx].Err = err
			continue
		}
		batchReferences[idx].From.Class = schema.ClassName(fromClass)
	}

	uniqueClass := map[string]struct{}{}
	type classAndShard struct {
		Class string
		Shard string
	}
	uniqueClassShard := map[string]classAndShard{}
	for idx := range batchReferences {
		if batchReferences[idx].Err != nil {
			continue
		}
		class := batchReferences[idx].From.Class.String()
		uniqueClass[class] = struct{}{}
		uniqueClassShard[class+"#"+batchReferences[idx].Tenant] = classAndShard{Class: class, Shard: batchReferences[idx].Tenant}
	}

	allClasses := make([]string, 0, len(uniqueClass))
	for classname := range uniqueClass {
		allClasses = append(allClasses, classname)
	}

	// All refs failed parsing/NS resolution: return per-ref errors as 200.
	// The authorizer rejects empty resources with "at least 1 resource
	// required", which the REST handler maps to 500.
	if len(allClasses) == 0 {
		return batchReferences, nil
	}

	fetchedClasses, err := b.schemaManager.GetCachedClass(ctx, principal, allClasses...)
	if err != nil {
		return nil, err
	}

	var pathsData []string
	for _, val := range uniqueClassShard {
		pathsData = append(pathsData, authorization.ShardsData(val.Class, val.Shard)...)
	}

	if err := b.authorizer.Authorize(ctx, principal, authorization.UPDATE, pathsData...); err != nil {
		return nil, err
	}

	b.metrics.BatchRefInc()
	defer b.metrics.BatchRefDec()

	return b.addReferences(ctx, principal, batchReferences, repl, fetchedClasses)
}

func (b *BatchManager) addReferences(ctx context.Context, principal *models.Principal,
	refs BatchReferences, repl *additional.ReplicationProperties, fetchedClasses map[string]versioned.Class,
) (BatchReferences, error) {
	if err := b.autodetectToClass(refs, fetchedClasses); err != nil {
		return nil, err
	}

	// MT validation must run after auto-detection — target class isn't
	// known beforehand in all cases. Per ref: QualifyRefTarget to get
	// qualified (for MT/authz/routing) + short (for the stored beacon).
	type classAndShard struct {
		Class string
		Shard string
	}
	uniqueClassShard := map[string]classAndShard{}
	var schemaVersion uint64
	for i, ref := range refs {
		if ref.Err != nil {
			continue
		}

		var qualifiedTarget string
		if ref.To != nil && ref.To.Class != "" {
			qualified, short, err := namespacing.QualifyRefTarget(
				principal, b.config.Config.Namespaces.Enabled,
				ref.From.Class.String(), ref.To.Class)
			if err != nil {
				refs[i].Err = err
				continue
			}
			qualifiedTarget = qualified
			ref.To.Class = short
		}

		if shouldValidateMultiTenantRef(ref.Tenant, ref.From, ref.To) {
			// Schema lookup needs qualified; storage struct ref.To stays short.
			targetQualified := *ref.To
			targetQualified.Class = qualifiedTarget
			classVersion, err := validateReferenceMultiTenancy(ctx, principal, b.schemaManager, b.vectorRepo, ref.From, &targetQualified, ref.Tenant, fetchedClasses)
			if err != nil {
				// Per-row Err but DON'T skip — authz still needs to track the
				// caller's intent (a missing READ must surface as 403, not be
				// hidden by an MT-mismatch). AddBatchReferences skips the row.
				refs[i].Err = err
			} else if classVersion > schemaVersion {
				schemaVersion = classVersion
			}
		}

		if qualifiedTarget == "" {
			// Classless beacon on a multi-target prop (autodetect can't pick).
			// On NS clusters the persisted ref would be un-deletable via the
			// delete handler's gate — reject. Non-NS keeps the legacy
			// behaviour (byte-exact compare on delete handles it).
			if b.config.Config.Namespaces.Enabled {
				refs[i].Err = fmt.Errorf("multi-target references require the class name in the target beacon url")
			}
			continue
		}
		uniqueClassShard[qualifiedTarget+"#"+ref.Tenant] = classAndShard{Class: qualifiedTarget, Shard: ref.Tenant}
	}

	shardsDataPaths := make([]string, 0, len(uniqueClassShard))
	for _, val := range uniqueClassShard {
		shardsDataPaths = append(shardsDataPaths, authorization.ShardsData(val.Class, val.Shard)...)
	}

	// target object existence is checked only with tenants enabled, but we
	// require the permission for everything to keep things simple. Skip
	// the call when every ref hit a pre-authz error (empty slice would
	// otherwise produce a 500 via "at least 1 resource required").
	if len(shardsDataPaths) > 0 {
		if err := b.authorizer.Authorize(ctx, principal, authorization.READ, shardsDataPaths...); err != nil {
			return nil, err
		}
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := b.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return nil, fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
	}
	if res, err := b.vectorRepo.AddBatchReferences(ctx, refs, repl, schemaVersion); err != nil {
		return nil, NewErrInternal("could not add batch request to connector: %v", err)
	} else {
		return res, nil
	}
}

func validateReferenceForm(refs []*models.BatchReference) error {
	if len(refs) == 0 {
		return fmt.Errorf("length cannot be 0, need at least one reference for batching")
	}

	return nil
}

func validateReferencesConcurrently(ctx context.Context, refs []*models.BatchReference, logger logrus.FieldLogger) BatchReferences {
	c := make(chan BatchReference, len(refs))
	wg := new(sync.WaitGroup)

	// Generate a goroutine for each separate request
	for i, ref := range refs {
		i := i
		ref := ref
		wg.Add(1)
		enterrors.GoWrapper(func() { validateReference(ctx, wg, ref, i, &c) }, logger)
	}

	wg.Wait()
	close(c)

	return referencesChanToSlice(c)
}

// autodetectToClass gets the class name of the referenced class through the schema definition
func (b *BatchManager) autodetectToClass(batchReferences BatchReferences, fetchedClasses map[string]versioned.Class) error {
	classPropTarget := make(map[string]string, len(batchReferences))
	for i, ref := range batchReferences {
		// Check Err before ref.To: a failed parse in validateReference sets
		// Err and leaves ref.To nil, so dereferencing first would panic.
		if ref.Err != nil || ref.To == nil || ref.To.Class != "" {
			continue
		}
		className := string(ref.From.Class)
		propName := schema.LowercaseFirstLetter(string(ref.From.Property))

		target, ok := classPropTarget[className+propName]
		if !ok {
			class := fetchedClasses[className]
			if class.Class == nil {
				batchReferences[i].Err = fmt.Errorf("source class %q not found in schema", className)
				continue
			}

			prop, err := schema.GetPropertyByName(class.Class, propName)
			if err != nil {
				batchReferences[i].Err = fmt.Errorf("property %s does not exist for class %s", propName, className)
				continue
			}
			if len(prop.DataType) > 1 {
				continue // can't auto-detect for multi-target
			}
			// Strip to short so the downstream resolveNS loop accepts it
			// (storage-shape rule, see namespacing.QualifyPropertyDataTypes).
			target = namespacing.StripQualification(prop.DataType[0])
			classPropTarget[className+propName] = target
		}
		batchReferences[i].To.Class = target
	}
	return nil
}

func validateReference(ctx context.Context,
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
	if target != nil {
		target.TargetID = strfmt.UUID(strings.ToLower(target.TargetID.String()))
	}

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
	tenant string, fetchedClasses map[string]versioned.Class,
) (uint64, error) {
	if source == nil || target == nil {
		return 0, fmt.Errorf("can't validate multi-tenancy for nil refs")
	}

	sourceClass, targetClass, schemaVersion, err := getReferenceClasses(
		ctx, principal, schemaManager, source.Class.String(), source.Property.String(), target.Class, fetchedClasses)
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
	classFrom, fromProperty, toClassName string, fetchedClasses map[string]versioned.Class,
) (sourceClass *models.Class, targetClass *models.Class, schemaVersion uint64, err error) {
	if classFrom == "" {
		err = fmt.Errorf("references involving a multi-tenancy enabled class " +
			"requires class name in the source beacon url")
		return sourceClass, targetClass, schemaVersion, err
	}

	fromClass := fetchedClasses[classFrom]
	if fromClass.Class == nil {
		err = fmt.Errorf("source class %q not found in schema", classFrom)
		return sourceClass, targetClass, schemaVersion, err
	}

	sourceClass = fromClass.Class
	schemaVersion = fromClass.Version

	// we can auto-detect the to class from the schema if it is a single target reference
	if toClassName == "" {
		refProp, err2 := schema.GetPropertyByName(sourceClass, fromProperty)
		if err2 != nil {
			err = fmt.Errorf("get source refprop %q: %w", classFrom, err2)
			return sourceClass, targetClass, schemaVersion, err
		}

		if len(refProp.DataType) != 1 {
			err = fmt.Errorf("multi-target references require the class name in the target beacon url")
			return sourceClass, targetClass, schemaVersion, err
		}
		toClassName = refProp.DataType[0]
	}

	toClass, ok := fetchedClasses[toClassName]
	if !ok {
		targetVclasses, err2 := schemaManager.GetCachedClass(ctx, principal, toClassName)
		if err2 != nil {
			err = fmt.Errorf("get target class %q: %w", toClassName, err2)
			return sourceClass, targetClass, schemaVersion, err
		}
		toClass = targetVclasses[toClassName]
		fetchedClasses[toClassName] = toClass
	}
	if toClass.Class == nil {
		err = fmt.Errorf("target class %q not found in schema", classFrom)
		return sourceClass, targetClass, schemaVersion, err
	}
	targetClass = toClass.Class

	return sourceClass, targetClass, schemaVersion, err
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

	return fmt.Errorf("%s", strings.Join(errorStrings, ", "))
}
