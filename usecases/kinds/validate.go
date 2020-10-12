//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ValidateThing without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateThing(ctx context.Context, principal *models.Principal,
	class *models.Thing) error {
	err := m.authorizer.Authorize(principal, "validate", "things")
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	err = m.validateThing(ctx, principal, class)
	if err != nil {
		return NewErrInvalidUserInput("invalid thing: %v", err)
	}

	return nil
}

// ValidateAction without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateAction(ctx context.Context, principal *models.Principal,
	class *models.Action) error {
	err := m.authorizer.Authorize(principal, "validate", "actions")
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	err = m.validateAction(ctx, principal, class)
	if err != nil {
		return NewErrInvalidUserInput("invalid action: %v", err)
	}

	return nil
}
