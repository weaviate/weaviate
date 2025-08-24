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

package common_filters

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func authorizePath(ctx context.Context, authorizer authorization.Authorizer, path *filters.Path, principal *models.Principal) error {
	if path == nil {
		return nil
	}
	if err := authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsData(path.Class.String())...); err != nil {
		return err
	}
	if path.Child != nil {
		return authorizePath(ctx, authorizer, path.Child, principal)
	}
	return nil
}

func AuthorizeFilters(ctx context.Context, authorizer authorization.Authorizer, clause *filters.Clause, principal *models.Principal) error {
	if clause == nil {
		return nil
	}
	if len(clause.Operands) == 0 {
		path := clause.On
		if path == nil {
			return fmt.Errorf("no path found in clause: %v", clause)
		}
		return authorizePath(ctx, authorizer, path, principal)
	} else {
		for _, operand := range clause.Operands {
			if err := AuthorizeFilters(ctx, authorizer, &operand, principal); err != nil {
				return err
			}
		}
	}
	return nil
}

func AuthorizeProperty(ctx context.Context, authorizer authorization.Authorizer, property *search.SelectProperty, principal *models.Principal) error {
	if property == nil {
		return nil
	}
	for _, ref := range property.Refs {
		if err := authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsData(ref.ClassName)...); err != nil {
			return err
		}
		for _, prop := range ref.RefProperties {
			if err := AuthorizeProperty(ctx, authorizer, &prop, principal); err != nil {
				return err
			}
		}
	}
	return nil
}
