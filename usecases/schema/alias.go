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
	if className != "" {
		name := schema.UppercaseClassName(className)
		class = h.schemaReader.ReadOnlyClass(name)
		if class == nil {
			// Optional class Filter not found. So return empty aliases list
			return []*models.Alias{}, nil
		}
	}
	aliases, err := h.schemaManager.GetAliases(ctx, alias, class)
	if err != nil {
		return nil, err
	}

	filteredAliases := filter.New[*models.Alias](h.Authorizer, h.config.Authorization.Rbac).Filter(
		ctx,
		h.logger,
		principal,
		aliases,
		authorization.READ,
		func(alias *models.Alias) string {
			class := className
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
	a, err := h.schemaManager.GetAlias(ctx, alias)
	if err != nil {
		if errors.Is(err, cschema.ErrAliasNotFound) {
			return nil, fmt.Errorf("alias %s not found: %w", alias, ErrNotFound)
		}
		return nil, err
	}

	if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.Aliases(a.Class, a.Alias)...); err != nil {
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
	qAlias, err := namespacing.QualifyForCreate(principal, h.config.Namespaces.Enabled, alias.Alias)
	if errors.Is(err, namespacing.ErrCreateRequiresNamespace) {
		return nil, 0, authzerrors.NewNamespaceForbidden(principal)
	}
	if err != nil {
		return nil, 0, err
	}
	qTarget, err := namespacing.QualifyForCreate(principal, h.config.Namespaces.Enabled, alias.Class)
	if err != nil {
		return nil, 0, err
	}
	alias.Alias = qAlias
	alias.Class = qTarget

	if err := h.Authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Aliases(alias.Class, alias.Alias)...); err != nil {
		return nil, 0, err
	}

	// alias should have same validation as collection.
	if _, err := schema.ValidateAliasName(originalAliasName); err != nil {
		return nil, 0, err
	}
	if _, err := schema.ValidateClassName(originalTargetName); err != nil {
		return nil, 0, err
	}

	class := h.schemaReader.ReadOnlyClass(alias.Class)
	version, err := h.schemaManager.CreateAlias(ctx, alias.Alias, class)
	if err != nil {
		return nil, 0, err
	}
	return &models.Alias{Alias: alias.Alias, Class: class.Class}, version, nil
}

func (h *Handler) UpdateAlias(ctx context.Context, principal *models.Principal,
	aliasName, targetClassName string,
) (*models.Alias, error) {
	targetClassName = schema.UppercaseClassName(targetClassName)
	aliasName = schema.UppercaseClassName(aliasName)
	err := h.Authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.Aliases(targetClassName, aliasName)...)
	if err != nil {
		return nil, err
	}
	aliases, err := h.schemaManager.GetAliases(ctx, aliasName, nil)
	if err != nil {
		return nil, err
	}

	if len(aliases) != 1 {
		return nil, fmt.Errorf("%w, no alias found with name: %s", ErrNotFound, aliasName)
	}

	alias := aliases[0]
	targetClass := h.schemaReader.ReadOnlyClass(targetClassName)

	_, err = h.schemaManager.ReplaceAlias(ctx, alias, targetClass)
	if err != nil {
		return nil, err
	}

	return &models.Alias{Alias: alias.Alias, Class: targetClass.Class}, nil
}

func (h *Handler) DeleteAlias(ctx context.Context, principal *models.Principal, aliasName string) error {
	aliasName = schema.UppercaseClassName(aliasName)

	a, err := h.schemaManager.GetAlias(ctx, aliasName)
	if err != nil {
		if errors.Is(err, cschema.ErrAliasNotFound) {
			return fmt.Errorf("alias %s not found: %w", aliasName, ErrNotFound)
		}
		return err
	}

	err = h.Authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Aliases(a.Class, a.Alias)...)
	if err != nil {
		return err
	}

	if _, err = h.schemaManager.DeleteAlias(ctx, aliasName); err != nil {
		return err
	}
	return nil
}
