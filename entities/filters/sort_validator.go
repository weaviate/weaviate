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

package filters

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

func ValidateSort(sch schema.Schema, className schema.ClassName, sort []Sort) error {
	if len(sort) == 0 {
		return errors.New("empty sort")
	}

	var errs []error
	for i := range sort {
		if err := validateSortClause(sch, className, sort[i]); err != nil {
			errs = append(errs, errors.Wrapf(err, "sort parameter at position %d", i))
		}
	}

	if len(errs) > 0 {
		return mergeErrs(errs)
	} else {
		return nil
	}
}

func validateSortClause(sch schema.Schema, className schema.ClassName, sort Sort) error {
	// validate current
	path, order := sort.Path, sort.Order

	if len(order) > 0 && order != "asc" && order != "desc" {
		return errors.Errorf(`invalid order parameter, `+
			`possible values are: ["asc", "desc"] not: "%s"`, order)
	}

	switch len(path) {
	case 0:
		return errors.New("path parameter cannot be empty")
	case 1:
		class := sch.FindClassByName(className)
		if class == nil {
			return errors.Errorf("class %q does not exist in schema",
				className)
		}
		propName := schema.PropertyName(path[0])
		if IsInternalProperty(propName) {
			// handle internal properties
			return nil
		}
		prop, err := sch.GetProperty(className, propName)
		if err != nil {
			return err
		}

		if isUUIDType(prop.DataType[0]) {
			return fmt.Errorf("prop %q is of type uuid/uuid[]: "+
				"sorting by uuid is currently not supported - if you believe it should be, "+
				"please open a feature request on github.com/weaviate/weaviate", prop.Name)
		}

		if schema.IsRefDataType(prop.DataType) {
			return errors.Errorf("sorting by reference not supported, "+
				"property %q is a ref prop to the class %q", propName, prop.DataType[0])
		}
		return nil
	default:
		return errors.New("sorting by reference not supported, " +
			"path must have exactly one argument")
	}
}
