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
func (m *Manager) HeadObject(ctx context.Context, principal *models.Principal, class string, id strfmt.UUID) (bool, error) {
	path := fmt.Sprintf("objects/%s", id)
	if class != "" {
		path = fmt.Sprintf("objects/%s/%s", class, id)
	}
	if err := m.authorizer.Authorize(principal, "head", path); err != nil {
		return false, fmt.Errorf("%w: %v", ErrAuthorization, err)
	}

	unlock, err := m.locks.LockConnector()
	if err != nil {
		return false, fmt.Errorf("%w: locking: %v", ErrServiceInternal, err)
	}
	defer unlock()

	ok, err := m.vectorRepo.Exists(ctx, class, id)
	if err != nil {
		return false, fmt.Errorf("%w: repo: %v", ErrServiceInternal, err)
	}
	return ok, nil
}
