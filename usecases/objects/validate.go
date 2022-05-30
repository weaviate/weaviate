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

package objects

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ValidateObject without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateObject(ctx context.Context, principal *models.Principal,
	class *models.Object) error {
	err := m.authorizer.Authorize(principal, "validate", "objects")
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	err = m.validateObject(ctx, principal, class)
	if err != nil {
		return NewErrInvalidUserInput("invalid object: %v", err)
	}

	return nil
}
