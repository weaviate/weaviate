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
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (m *Manager) updateRefVector(ctx context.Context,
	className string, id strfmt.UUID,
) error {
	if m.modulesProvider.UsingRef2Vec(className) {
		parent, err := m.vectorRepo.Object(ctx, className,
			id, search.SelectProperties{}, additional.Properties{})
		if err != nil {
			return fmt.Errorf("find parent '%s/%s': %w",
				className, id, err)
		}

		obj := parent.Object()

		if err := m.modulesProvider.UpdateVector(
			ctx, obj, nil, m.findObject, m.logger); err != nil {
			return fmt.Errorf("calculate ref vector for '%s/%s': %w",
				className, id, err)
		}

		if err := m.vectorRepo.PutObject(ctx, obj, obj.Vector); err != nil {
			return fmt.Errorf("put object: %w", err)
		}

		return nil
	}

	// nothing to do
	return nil
}

// TODO: remove this method and just pass m.vectorRepo.Object to
// m.modulesProvider.UpdateVector when m.vectorRepo.ObjectByID
// is finally removed
func (m *Manager) findObject(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties,
) (*search.Result, error) {
	// to support backwards compat
	if class == "" {
		return m.vectorRepo.ObjectByID(ctx, id, props, addl)
	}
	return m.vectorRepo.Object(ctx, class, id, props, addl)
}

// TODO: remove this method and just pass b.vectorRepo.Object to
// b.modulesProvider.UpdateVector when b.vectorRepo.ObjectByID
// is finally removed
func (b *BatchManager) findObject(ctx context.Context, class string,
	id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties,
) (*search.Result, error) {
	// to support backwards compat
	if class == "" {
		return b.vectorRepo.ObjectByID(ctx, id, props, addl)
	}
	return b.vectorRepo.Object(ctx, class, id, props, addl)
}
