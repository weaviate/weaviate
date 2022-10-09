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

package vectorizer

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/modules/ref2vec-centroid/config"
)

type calcFn func(vecs ...[]float32) ([]float32, error)

type Vectorizer struct {
	config       *config.Config
	calcFn       calcFn
	findObjectFn modulecapabilities.FindObjectFn
}

func New(cfg moduletools.ClassConfig, findFn modulecapabilities.FindObjectFn) *Vectorizer {
	v := &Vectorizer{
		config:       config.New(cfg),
		findObjectFn: findFn,
	}

	switch v.config.CalculationMethod() {
	case config.MethodMean:
		v.calcFn = calculateMean
	default:
		v.calcFn = calculateMean
	}

	return v
}

func (v *Vectorizer) Object(ctx context.Context, obj *models.Object) error {
	props := v.config.ReferenceProperties()

	refVecs, err := v.referenceVectorSearch(ctx, obj, props)
	if err != nil {
		return err
	}

	if len(refVecs) == 0 {
		obj.Vector = nil
		return nil
	}

	vec, err := v.calcFn(refVecs...)
	if err != nil {
		return fmt.Errorf("calculate vector: %w", err)
	}

	obj.Vector = vec
	return nil
}

func (v *Vectorizer) referenceVectorSearch(ctx context.Context,
	obj *models.Object, refProps map[string]struct{},
) ([][]float32, error) {
	var refVecs [][]float32
	props := obj.Properties.(map[string]interface{})

	// use the ids from parent's beacons to find the referenced objects
	beacons := beaconsForVectorization(props, refProps)
	for _, beacon := range beacons {
		res, err := v.findReferenceObject(ctx, beacon)
		if err != nil {
			return nil, err
		}

		// if the ref'd object has a vector, we grab it.
		// these will be used to compute the parent's
		// vector eventually
		if res.Vector != nil {
			refVecs = append(refVecs, res.Vector)
		}
	}

	return refVecs, nil
}

func (v *Vectorizer) findReferenceObject(ctx context.Context, beacon strfmt.URI) (res *search.Result, err error) {
	ref, err := crossref.Parse(beacon.String())
	if err != nil {
		return nil, fmt.Errorf("parse beacon %q: %w", beacon, err)
	}

	res, err = v.findObjectFn(ctx, ref.Class, ref.TargetID,
		search.SelectProperties{}, additional.Properties{})
	if err != nil || res == nil {
		if err == nil {
			err = fmt.Errorf("not found")
		}
		err = fmt.Errorf("find object with beacon %q': %w", beacon, err)
	}
	return
}

func beaconsForVectorization(allProps map[string]interface{},
	targetRefProps map[string]struct{},
) []strfmt.URI {
	var beacons []strfmt.URI

	// add any refs that were supplied as a part of the parent
	// object, like when caller is AddObject/UpdateObject
	for prop, val := range allProps {
		if _, ok := targetRefProps[prop]; ok {
			refs := val.(models.MultipleRef)
			for _, ref := range refs {
				beacons = append(beacons, ref.Beacon)
			}
		}
	}

	return beacons
}
