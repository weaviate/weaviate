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

package search

import (
	"net/http"
	"reflect"
	"strings"
	"time"

	dbinverted "github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// validateWhere rejects filters the engine would otherwise answer wrongly or
// untyped (a 500): operand count, value shape, date format, null-state
// indexing and reference paths that leave the schema.
func (h *Handler) validateWhere(clause *filters.Clause, class *models.Class, getClass classGetterFunc) *APIError {
	if clause == nil {
		return nil
	}

	switch clause.Operator {
	case filters.OperatorAnd, filters.OperatorOr:
		if len(clause.Operands) == 0 {
			return newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: operator %s needs at least one operand", clause.Operator.Name())
		}
	case filters.OperatorNot:
		if len(clause.Operands) != 1 {
			return newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: operator Not takes exactly one operand, got %d", len(clause.Operands))
		}
	default:
		// value operators are checked below
	}
	for i := range clause.Operands {
		if apiErr := h.validateWhere(&clause.Operands[i], class, getClass); apiErr != nil {
			return apiErr
		}
	}
	if clause.On == nil {
		return nil
	}

	leafClass, apiErr := h.validateWherePath(clause.On, class, getClass)
	if apiErr != nil {
		return apiErr
	}
	return validateWhereValue(clause, leafClass)
}

// validateWherePath follows a reference path hop by hop: every inner segment
// must be a reference property whose declared targets include the class named
// next. The class of the last hop is returned for the value checks.
func (h *Handler) validateWherePath(p *filters.Path, class *models.Class, getClass classGetterFunc) (*models.Class, *APIError) {
	for p.Child != nil {
		name := schema.LowercaseFirstLetter(p.Property.String())
		prop, err := schema.GetPropertyByName(class, name)
		if err != nil {
			return nil, &APIError{Status: http.StatusBadRequest, Err: err}
		}
		if !schema.IsRefDataType(prop.DataType) {
			return nil, newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: %q is not a reference property, a path can only continue through references", name)
		}

		target := p.Child.Class.String()
		if !contains(prop.DataType, target) {
			return nil, newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: reference %q does not target collection %q. Available target collections %v",
				name, target, prop.DataType)
		}

		// a denial stays 403; a declared target that is missing is a schema
		// race, so a bad request, not a 404 for a collection the caller never
		// asked for
		next, err := getClass(target)
		if err != nil {
			apiErr := statusFromError(err)
			if apiErr.Status != http.StatusForbidden {
				apiErr = &APIError{Status: http.StatusBadRequest, Err: err}
			}
			return nil, apiErr
		}
		class, p = next, p.Child
	}
	return class, nil
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func validateWhereValue(clause *filters.Clause, class *models.Class) *APIError {
	v := clause.Value
	if v == nil {
		return nil
	}
	isArray := reflect.ValueOf(v.Value).Kind() == reflect.Slice
	op := clause.Operator

	switch op {
	case filters.ContainsAny, filters.ContainsAll, filters.ContainsNone:
		if !isArray {
			return newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: operator %s needs an array value (e.g. valueTextArray)", op.Name())
		}
	default:
		if isArray {
			return newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: operator %s takes a single value, got an array", op.Name())
		}
	}

	if op == filters.OperatorLike {
		if s, ok := v.Value.(string); ok && strings.TrimSpace(s) == "" {
			return newAPIError(http.StatusBadRequest, "invalid 'where' filter: operator Like needs a non-empty pattern")
		}
	}

	if v.Type == schema.DataTypeDate {
		if apiErr := validateDates(v.Value); apiErr != nil {
			return apiErr
		}
	}

	if op == filters.OperatorIsNull && class != nil {
		if class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.IndexNullState {
			return newAPIError(http.StatusUnprocessableEntity,
				"operator IsNull needs indexNullState enabled in the inverted index config of collection %s", class.Class)
		}
		// the null-state bucket only exists for indexed properties
		leaf := leafProperty(clause.On)
		if prop, err := schema.GetPropertyByName(class, leaf); err == nil && !dbinverted.HasFilterableIndex(prop) {
			return newAPIError(http.StatusUnprocessableEntity,
				"operator IsNull on property %q needs indexFilterable enabled on it", leaf)
		}
	}

	return nil
}

func leafProperty(p *filters.Path) string {
	for p.Child != nil {
		p = p.Child
	}
	return schema.LowercaseFirstLetter(p.Property.String())
}

func validateDates(value any) *APIError {
	var dates []string
	switch d := value.(type) {
	case string:
		dates = []string{d}
	case []string:
		dates = d
	}
	for _, d := range dates {
		if _, err := time.Parse(time.RFC3339, d); err != nil {
			return newAPIError(http.StatusBadRequest,
				"invalid 'where' filter: %q is not an RFC3339 date (e.g. 2006-01-02T15:04:05Z)", d)
		}
	}
	return nil
}
