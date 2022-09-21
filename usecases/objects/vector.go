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
			ctx, obj, m.vectorRepo, m.logger); err != nil {
			return fmt.Errorf("calculate ref vector for '%s/%s': %w",
				className, id, err)
		}

		if err := m.vectorRepo.PutObject(ctx, obj, obj.Vector); err != nil {
			return fmt.Errorf("put object: %s", err)
		}

		return nil
	}

	// nothing to do
	return nil
}

//func (m *Manager) vectorizeAndPutObject(ctx context.Context,
//	object *models.Object, principal *models.Principal,
//	refs ...*models.SingleRef,
//) error {
//	err := newVectorObtainer(m.vectorizerProvider,
//		m.schemaManager, m.logger).Do(ctx, object, principal)
//	if err != nil {
//		return err
//	}
//
//	err = m.vectorRepo.PutObject(ctx, object, object.Vector)
//	if err != nil {
//		return NewErrInternal("store: %v", err)
//	}
//
//	return nil
//}

//func getVectorizerOfClass(mgr schemaManager, className string,
//	principal *models.Principal,
//) (string, interface{}, error) {
//	s, err := mgr.GetSchema(principal)
//	if err != nil {
//		return "", nil, err
//	}
//
//	class := s.FindClassByName(schema.ClassName(className))
//	if class == nil {
//		// this should be impossible by the time this method gets called, but let's
//		// be 100% certain
//		return "", nil, errors.Errorf("class %s not present", className)
//	}
//
//	return class.Vectorizer, class.VectorIndexConfig, nil
//}
