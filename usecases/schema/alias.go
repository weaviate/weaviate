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

package schema

import (
	"context"
	"errors"
	"fmt"

	cschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/filter"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (h *Handler) GetAliases(ctx context.Context, principal *models.Principal, alias, className string) ([]*models.Alias, error) {
	var class *models.Class
	var qClass string
	if className != "" {
		var err error
		qClass, err = namespacing.QualifyClass(principal, h.qualifier, className)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrValidation, err)
		}
		// No existence lookup: a miss would take the eventual-consistency retry
		// path and time differently from a hit for callers not allowed to know.
		class = &models.Class{Class: qClass}
	}
	aliases, err := h.schemaManager.GetAliases(ctx, alias, class)
	if err != nil {
		return nil, err
	}
	aliases = namespacing.KeepOwnNamespace(principal, aliases, func(alias *models.Alias) string {
		return namespacing.NamespaceFromQualified(alias.Alias)
	})

	filteredAliases := filter.New[*models.Alias](h.Authorizer, h.config.Authorization.Rbac).Filter(
		ctx,
		principal,
		aliases,
		authorization.READ,
		func(alias *models.Alias) string {
			class := qClass
			if class == "" {
				class = alias.Class
			}
			return authorization.Aliases(class, alias.Alias)[0]
		},
	)

	return filteredAliases, nil
}

func (h *Handler) GetAlias(ctx context.Context, principal *models.Principal, alias string) (*models.Alias, error) {
	alias = schema.UppercaseClassName(alias)
	qAlias, err := namespacing.QualifyClass(principal, h.qualifier, alias)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	alias = qAlias

	a, err := h.schemaManager.GetAlias(ctx, alias)
	if err != nil {
		if errors.Is(err, cschema.ErrAliasNotFound) {
			return nil, h.aliasNotFound(alias)
		}
		return nil, err
	}

	if err := h.authorizeExistingAlias(ctx, principal, authorization.READ, a, alias); err != nil {
		return nil, err
	}

	return a, nil
}

func (h *Handler) AddAlias(ctx context.Context, principal *models.Principal,
	alias *models.Alias,
) (*models.Alias, uint64, error) {
	alias.Class = schema.UppercaseClassName(alias.Class)
	alias.Alias = schema.UppercaseClassName(alias.Alias)

	// Captured for the entity-name validators, which forbid ":".
	originalAliasName := alias.Alias
	originalTargetName := alias.Class
	qAlias, err := namespacing.QualifyForCreate(principal, h.qualifier, alias.Alias, "alias")
	if errors.Is(err, namespacing.ErrCreateRequiresNamespace) {
		return nil, 0, authzerrors.NewNamespaceForbidden(principal)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	qTarget, err := namespacing.QualifyForCreate(principal, h.qualifier, alias.Class, "class")
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	alias.Alias = qAlias
	alias.Class = qTarget

	if err := h.Authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Aliases(alias.Class, alias.Alias)...); err != nil {
		return nil, 0, err
	}

	// alias should have same validation as collection.
	if _, err := schema.ValidateAliasName(originalAliasName); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	if _, err := schema.ValidateClassName(originalTargetName); err != nil {
		return nil, 0, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	class := h.schemaReader.ReadOnlyClass(alias.Class)
	if class == nil {
		return nil, 0, fmt.Errorf("%w: target class %q does not exist", ErrValidation, alias.Class)
	}
	version, err := h.schemaManager.CreateAlias(ctx, alias.Alias, class)
	if err != nil {
		if errors.Is(err, cschema.ErrBadRequest) ||
			errors.Is(err, cschema.ErrAliasExists) ||
			errors.Is(err, cschema.ErrClassNotFound) {
			return nil, 0, fmt.Errorf("%w: %w", ErrValidation, err)
		}
		return nil, 0, err
	}
	return &models.Alias{Alias: alias.Alias, Class: class.Class}, version, nil
}

func (h *Handler) UpdateAlias(ctx context.Context, principal *models.Principal,
	aliasName, targetClassName string,
) (*models.Alias, error) {
	targetClassName = schema.UppercaseClassName(targetClassName)
	aliasName = schema.UppercaseClassName(aliasName)
	qAlias, err := namespacing.QualifyClass(principal, h.qualifier, aliasName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	qTarget, err := namespacing.QualifyClass(principal, h.qualifier, targetClassName)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	aliasName = qAlias
	targetClassName = qTarget

	if err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Aliases(targetClassName, aliasName)...); err != nil {
		return nil, err
	}
	if err := namespacing.ValidateAliasTarget(aliasName, targetClassName); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}
	aliases, err := h.schemaManager.GetAliases(ctx, aliasName, nil)
	if err != nil {
		return nil, err
	}

	if len(aliases) != 1 {
		return nil, h.aliasNotFound(aliasName)
	}

	alias := aliases[0]
	// Moving an alias also needs update_aliases on the collection it leaves.
	if err := h.authorizeExistingAlias(ctx, principal, authorization.UPDATE, alias, aliasName); err != nil {
		return nil, err
	}

	targetClass := h.schemaReader.ReadOnlyClass(targetClassName)
	if targetClass == nil {
		return nil, fmt.Errorf("%w: target class %q does not exist", ErrValidation, targetClassName)
	}

	_, err = h.schemaManager.ReplaceAlias(ctx, alias, targetClass)
	if err != nil {
		if errors.Is(err, cschema.ErrBadRequest) ||
			errors.Is(err, cschema.ErrClassNotFound) ||
			errors.Is(err, cschema.ErrAliasNotFound) {
			return nil, fmt.Errorf("%w: %w", ErrValidation, err)
		}
		return nil, err
	}

	return &models.Alias{Alias: alias.Alias, Class: targetClass.Class}, nil
}

// authorizeExistingAlias authorizes verb on an alias that exists. A caller who
// cannot read the alias gets the missing-alias answer instead of Forbidden.
func (h *Handler) authorizeExistingAlias(ctx context.Context, principal *models.Principal,
	verb string, a *models.Alias, aliasName string,
) error {
	resources := authorization.Aliases(a.Class, a.Alias)
	err := h.Authorizer.Authorize(ctx, principal, verb, resources...)
	if !errors.As(err, &authzerrors.Forbidden{}) {
		return err
	}
	if verb == authorization.READ {
		return h.aliasNotFound(aliasName)
	}
	readErr := h.Authorizer.AuthorizeSilent(ctx, principal, authorization.READ, resources...)
	if readErr == nil {
		return err
	}
	if errors.As(readErr, &authzerrors.Forbidden{}) {
		return h.aliasNotFound(aliasName)
	}
	return readErr
}

func (h *Handler) aliasNotFound(aliasName string) error {
	return fmt.Errorf("%w, no alias found with name: %s", ErrNotFound, aliasName)
}

func (h *Handler) DeleteAlias(ctx context.Context, principal *models.Principal, aliasName string) error {
	aliasName = schema.UppercaseClassName(aliasName)
	qAlias, err := namespacing.QualifyClass(principal, h.qualifier, aliasName)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrValidation, err)
	}
	aliasName = qAlias

	a, err := h.schemaManager.GetAlias(ctx, aliasName)
	if err != nil {
		if errors.Is(err, cschema.ErrAliasNotFound) {
			return h.aliasNotFound(aliasName)
		}
		return err
	}

	if err := h.authorizeExistingAlias(ctx, principal, authorization.DELETE, a, aliasName); err != nil {
		return err
	}

	if _, err := h.schemaManager.DeleteAlias(ctx, aliasName); err != nil {
		if errors.Is(err, cschema.ErrAliasNotFound) {
			return fmt.Errorf("alias %s not found: %w", aliasName, ErrNotFound)
		}
		if errors.Is(err, cschema.ErrBadRequest) {
			return fmt.Errorf("%w: %w", ErrValidation, err)
		}
		return err
	}
	return nil
}
