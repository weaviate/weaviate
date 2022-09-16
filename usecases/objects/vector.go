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
	"path"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
)

func (m *Manager) findParentAndVectorizeRefs(ctx context.Context,
	principal *models.Principal, className string, id strfmt.UUID,
	refs ...*models.SingleRef,
) error {
	parent, err := m.vectorRepo.ObjectByID(
		ctx, id, search.SelectProperties{}, additional.Properties{})
	if err != nil {
		return fmt.Errorf("find parent '%s/%s': %w", className, id, err)
	}

	if err := m.vectorizeRefsAndPutObject(
		ctx, parent.Object(), principal, refs); err != nil {
		return fmt.Errorf("calculate ref vector for parent '%s/%s': %w", className, id, err)
	}

	return nil
}

func (m *Manager) vectorizeRefsAndPutObject(ctx context.Context, parent *models.Object,
	principal *models.Principal, refs models.MultipleRef,
) error {
	ref2VecPropNames, err := m.modulesProvider.TargetReferenceProperties(parent.Class)
	if err != nil {
		return fmt.Errorf("target reference props: %w", err)
	}

	refVecs, err := m.getReferenceVectorsFromParent(
		ctx, parent.Properties, refs, ref2VecPropNames)
	if err != nil {
		return fmt.Errorf("get reference vectors: %w", err)
	}

	err = newReferenceVectorObtainer(
		m.referenceVectorProvider, m.schemaManager,
		m.logger).Do(ctx, parent, principal,
		refVecs...)
	if err != nil {
		return fmt.Errorf("obtain centroid: %w", err)
	}

	err = m.vectorRepo.PutObject(ctx, parent, parent.Vector)
	if err != nil {
		return fmt.Errorf("put '%s/%s': %w", parent.Class, parent.ID, err)
	}

	return nil
}

func (m *Manager) getReferenceVectorsFromParent(ctx context.Context,
	objProps models.PropertySchema, refs models.MultipleRef,
	referencePropNames map[string]struct{},
) (refVecs [][]float32, err error) {
	props := objProps.(map[string]interface{})

	// use the ids from parent's beacons to find the referenced objects
	beacons := beaconsForVectorization(props, referencePropNames, refs)
	for _, beacon := range beacons {
		res, searchErr := m.vectorRepo.ObjectByID(ctx, strfmt.UUID(path.Base(beacon)),
			search.SelectProperties{}, additional.Properties{})
		if searchErr != nil {
			err = fmt.Errorf("find object with beacon %q': %w", beacon, searchErr)
			return
		}

		// if the ref'd object has a vector, we grab it.
		// these will be used to compute the parent's
		// vector eventually
		if res.Vector != nil {
			refVecs = append(refVecs, res.Vector)
		}
	}

	return
}

func (m *Manager) vectorizeAndPutObject(ctx context.Context,
	object *models.Object, principal *models.Principal,
	refs ...*models.SingleRef,
) error {
	calcVecWithRefs, err := m.shouldRecalculateVectorWithRefs(object.Class, principal)
	if err != nil {
		return err
	}

	if calcVecWithRefs {
		return m.vectorizeRefsAndPutObject(ctx, object, principal, refs)
	}

	err = newVectorObtainer(m.vectorizerProvider,
		m.schemaManager, m.logger).Do(ctx, object, principal)
	if err != nil {
		return err
	}

	err = m.vectorRepo.PutObject(ctx, object, object.Vector)
	if err != nil {
		return NewErrInternal("store: %v", err)
	}

	return nil
}

func (m *Manager) shouldRecalculateVectorWithRefs(className string, principal *models.Principal) (bool, error) {
	vzrName, _, err := getVectorizerOfClass(m.schemaManager, className, principal)
	if err != nil {
		return false, fmt.Errorf("get class vectorizer: %w", err)
	}

	return m.modulesProvider.UsingRef2Vec(vzrName), nil
}

func getVectorizerOfClass(mgr schemaManager, className string,
	principal *models.Principal,
) (string, interface{}, error) {
	s, err := mgr.GetSchema(principal)
	if err != nil {
		return "", nil, err
	}

	class := s.FindClassByName(schema.ClassName(className))
	if class == nil {
		// this should be impossible by the time this method gets called, but let's
		// be 100% certain
		return "", nil, errors.Errorf("class %s not present", className)
	}

	return class.Vectorizer, class.VectorIndexConfig, nil
}

// beaconsForVectorization filters an objects properties for those which
// are targeted by ref2vec. when such a prop is found, we grab its beacon
// so that we can use the contained id for finding the referenced objects
// later
func beaconsForVectorization(allProps map[string]interface{},
	targetRefProps map[string]struct{}, refs models.MultipleRef,
) []string {
	var beacons []string

	// add any refs that were supplied outside the parent object,
	// like when caller is AddObjectReference/DeleteObjectReference
	for _, ref := range refs {
		beacons = append(beacons, ref.Beacon.String())
	}

	// add any refs that were supplied as a part of the parent
	// object, like when caller is AddObject/UpdateObject
	for prop, val := range allProps {
		if _, ok := targetRefProps[prop]; ok {
			refs := val.(models.MultipleRef)
			for _, ref := range refs {
				beacons = append(beacons, ref.Beacon.String())
			}
		}
	}

	return beacons
}
