/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/go-openapi/strfmt"
)

type deleteAndGetRepo interface {
	deleteRepo
	getRepo
}

type deleteRepo interface {
	// TODO: Delete unnecessary 2nd arg
	DeleteThing(ctx context.Context, thing *models.Thing, UUID strfmt.UUID) error
	DeleteAction(ctx context.Context, thing *models.Action, UUID strfmt.UUID) error
}

// DeleteAction Class Instance from the conncected DB
func (m *Manager) DeleteAction(ctx context.Context, id strfmt.UUID) error {
	unlock, err := m.locks.LockConnector()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.deleteActionFromRepo(ctx, id)
}

func (m *Manager) deleteActionFromRepo(ctx context.Context, id strfmt.UUID) error {
	_, err := m.getActionFromRepo(ctx, id)
	if err != nil {
		return err
	}

	m.repo.DeleteAction(ctx, nil, id)
	if err != nil {
		return newErrInternal("could not delete action: %v", err)
	}

	return nil
}

// DeleteThing Class Instance from the conncected DB
func (m *Manager) DeleteThing(ctx context.Context, id strfmt.UUID) error {
	unlock, err := m.locks.LockConnector()
	if err != nil {
		return newErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.deleteThingFromRepo(ctx, id)
}

func (m *Manager) deleteThingFromRepo(ctx context.Context, id strfmt.UUID) error {

	_, err := m.getThingFromRepo(ctx, id)
	if err != nil {
		return err
	}

	m.repo.DeleteThing(ctx, nil, id)
	if err != nil {
		return newErrInternal("could not delete thing: %v", err)
	}

	return nil
}
