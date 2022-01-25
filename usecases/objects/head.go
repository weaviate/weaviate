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
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

// HeadObject check object's existence in the conncected DB
func (m *Manager) HeadObject(ctx context.Context, principal *models.Principal, id strfmt.UUID) (bool, error) {
	err := m.authorizer.Authorize(principal, "head", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return false, err
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return false, NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.checkObjectInRepo(ctx, id)
}

func (m *Manager) checkObjectInRepo(ctx context.Context, id strfmt.UUID) (bool, error) {
	exists, err := m.vectorRepo.Exists(ctx, id)
	if err != nil {
		return false, NewErrInternal("could not check object's existence: %v", err)
	}

	return exists, nil
}
