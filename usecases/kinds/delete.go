//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
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
func (m *Manager) DeleteAction(ctx context.Context, principal *models.Principal, id strfmt.UUID) error {
	err := m.authorizer.Authorize(principal, "delete", fmt.Sprintf("actions/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.deleteActionFromRepo(ctx, id)
}

func (m *Manager) deleteActionFromRepo(ctx context.Context, id strfmt.UUID) error {
	action, err := m.getActionFromRepo(ctx, id)
	if err != nil {
		return err
	}

	if !m.config.Config.EsvectorOnly {
		err = m.repo.DeleteAction(ctx, nil, id)
		if err != nil {
			return NewErrInternal("could not delete action: %v", err)
		}
	}

	err = m.vectorRepo.DeleteAction(ctx, action.Class, id)
	if err != nil {
		return NewErrInternal("could not delete action from vector repo: %v", err)
	}

	return nil
}

// DeleteThing Class Instance from the conncected DB
func (m *Manager) DeleteThing(ctx context.Context, principal *models.Principal, id strfmt.UUID) error {
	err := m.authorizer.Authorize(principal, "delete", fmt.Sprintf("things/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return NewErrInternal("could not aquire lock: %v", err)
	}
	defer unlock()

	return m.deleteThingFromRepo(ctx, id)
}

func (m *Manager) deleteThingFromRepo(ctx context.Context, id strfmt.UUID) error {

	thing, err := m.getThingFromRepo(ctx, id)
	if err != nil {
		return err
	}

	if !m.config.Config.EsvectorOnly {
		err = m.repo.DeleteThing(ctx, nil, id)
		if err != nil {
			return NewErrInternal("could not delete thing: %v", err)
		}
	}

	err = m.vectorRepo.DeleteThing(ctx, thing.Class, id)
	if err != nil {
		return NewErrInternal("could not delete thing from vector repo: %v", err)
	}

	return nil
}
