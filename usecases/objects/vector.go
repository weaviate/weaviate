//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (m *Manager) updateRefVector(ctx context.Context,
	className string, id strfmt.UUID, tenant string, class *models.Class, schemaVersion uint64,
) error {
	if m.modulesProvider.UsingRef2Vec(className) {
		parent, err := m.vectorRepo.Object(ctx, className, id,
			search.SelectProperties{}, additional.Properties{}, nil, tenant)
		if err != nil {
			return fmt.Errorf("find parent '%s/%s': %w",
				className, id, err)
		}

		obj := parent.Object()

		if err := m.modulesProvider.UpdateVector(ctx, obj, class,
			findObjectFor(m.vectorRepo, m.qualifier, class.Class), m.logger); err != nil {
			return fmt.Errorf("calculate ref vector for '%s/%s': %w",
				className, id, err)
		}

		if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
			return fmt.Errorf("error waiting for local schema to catch up to version %d: %w", schemaVersion, err)
		}

		vectors, multiVectors, err := dto.GetVectors(obj.Vectors)
		if err != nil {
			return fmt.Errorf("put object: cannot get vectors: %w", err)
		}

		if err := m.vectorRepo.PutObject(ctx, obj, obj.Vector, vectors, multiVectors, nil, schemaVersion); err != nil {
			return fmt.Errorf("put object: %w", err)
		}

		return nil
	}

	// nothing to do
	return nil
}

// findObjectFor returns the lookup a vectorizer module gets when it vectorizes
// an object of sourceClass. A stored beacon carries the short class, so the
// lookup qualifies the class with sourceClass's namespace before it reads.
func findObjectFor(repo VectorRepo, q namespacing.Qualifier, sourceClass string) modulecapabilities.FindObjectFn {
	return func(ctx context.Context, class string, id strfmt.UUID,
		props search.SelectProperties, addl additional.Properties, tenant string,
	) (*search.Result, error) {
		// A beacon without a class reads by id. ObjectByID searches only the
		// collections outside any namespace, the only ones holding such beacons.
		if class == "" {
			return repo.ObjectByID(ctx, id, props, addl, tenant)
		}
		qualified, _, err := q.QualifyRefTarget(sourceClass, class)
		if err != nil {
			return nil, err
		}
		return repo.Object(ctx, qualified, id, props, addl, nil, tenant)
	}
}
