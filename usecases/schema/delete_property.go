//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
)

// DeleteClassProperty from existing Schema
func (m *Manager) DeleteClassProperty(ctx context.Context, principal *models.Principal,
	class string, property string) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/semi-technologies/weaviate/issues/973 for details.")
	// return m.deleteClassProperty(ctx, class, property, kind.Action)
}
