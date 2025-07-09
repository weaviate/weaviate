//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (h *Handler) GetAliases(ctx context.Context, principal *models.Principal, alias, className string) ([]*models.Alias, error) {
	if err := h.Authorizer.Authorize(ctx, principal, authorization.READ, authorization.Aliases(className, alias)...); err != nil {
		return nil, err
	}
	var class *models.Class
	if className != "" {
		name := schema.UppercaseClassName(className)
		class = h.schemaReader.ReadOnlyClass(name)
	}
	aliases, err := h.schemaManager.GetAliases(ctx, alias, class)
	if err != nil {
		return nil, err
	}
	return aliases, nil
}

func (h *Handler) AddAlias(ctx context.Context, principal *models.Principal,
	alias *models.Alias,
) (*models.Alias, uint64, error) {
	alias.Class = schema.UppercaseClassName(alias.Class)
	alias.Alias = schema.UppercaseClassName(alias.Alias)
	err := h.Authorizer.Authorize(ctx, principal, authorization.CREATE, authorization.Aliases(alias.Class, alias.Alias)...)
	if err != nil {
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
	err := h.Authorizer.Authorize(ctx, principal, authorization.DELETE, authorization.Aliases("", aliasName)...)
	if err != nil {
		return err
	}

	aliases, err := h.schemaManager.GetAliases(ctx, aliasName, nil)
	if err != nil {
		return err
	}
	if len(aliases) == 0 {
		return fmt.Errorf("alias not found: %w", ErrNotFound)
	}

	if _, err = h.schemaManager.DeleteAlias(ctx, aliasName); err != nil {
		return err
	}
	return nil
}
