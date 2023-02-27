//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/schema"
)

func ValidateScroll(className schema.ClassName, scroll *Scroll, offset int, filters *LocalFilter, sort []Sort) error {
	if className == "" {
		return fmt.Errorf("class parameter cannot be empty")
	}
	if offset > 0 || filters != nil || sort != nil {
		var params []string
		if offset > 0 {
			params = append(params, "offset")
		}
		if filters != nil {
			params = append(params, "where")
		}
		if sort != nil {
			params = append(params, "sort")
		}
		return fmt.Errorf("%s cannot be set with after and limit parameters", strings.Join(params, ","))
	}
	if scroll.After != "" {
		if _, err := uuid.Parse(scroll.After); err != nil {
			return errors.Wrapf(err, "after parameter '%s' is not a valid uuid", scroll.After)
		}
	}
	return nil
}
