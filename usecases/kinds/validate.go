/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package kinds

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ValidateThing without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateThing(ctx context.Context, class *models.Thing) error {
	unlock, err := m.locks.LockConnector()
	if err != nil {
		return newErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	err = m.validateThing(ctx, class)
	if err != nil {
		return newErrInvalidUserInput("invalid thing: %v", err)
	}

	return nil
}

// ValidateAction without adding it to the database. Can be used in UIs for
// async validation before submitting
func (m *Manager) ValidateAction(ctx context.Context, class *models.Action) error {
	unlock, err := m.locks.LockConnector()
	if err != nil {
		return newErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	err = m.validateAction(ctx, class)
	if err != nil {
		return newErrInvalidUserInput("invalid action: %v", err)
	}

	return nil
}
