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
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func AuthorizeFilters(authorizer authorization.Authorizer, clause *filters.Clause, principal *models.Principal) error {
	if clause == nil {
		return nil
	}
	if len(clause.Operands) == 0 {
		path := clause.On
		if path == nil {
			return fmt.Errorf("no path found in clause: %v", clause)
		}
		return authorizer.Authorize(principal, authorization.READ, authorization.CollectionsData(path.Class.String())...)
	} else {
		for _, operand := range clause.Operands {
			if err := AuthorizeFilters(authorizer, &operand, principal); err != nil {
				return err
			}
		}
	}
	return nil
}

func AuthorizeProperty(authorizer authorization.Authorizer, property *search.SelectProperty, principal *models.Principal) error {
	if property == nil {
		return nil
	}
	for _, ref := range property.Refs {
		if err := authorizer.Authorize(principal, authorization.READ, authorization.CollectionsData(ref.ClassName)...); err != nil {
			return err
		}
		for _, prop := range ref.RefProperties {
			if err := AuthorizeProperty(authorizer, &prop, principal); err != nil {
				return err
			}
		}
	}
	return nil
}
