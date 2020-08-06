//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package db

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/filters"
	libfilters "github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/classification"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
)

// TODO: why is this logic in the persistence package? This is business-logic,
// move out of here!
func (db *DB) GetUnclassified(ctx context.Context, kind kind.Kind, class string,
	properties []string, filter *libfilters.LocalFilter) ([]search.Result, error) {

	mergedFilter := mergeUserFilterWithRefCountFilter(filter, class, properties,
		libfilters.OperatorEqual, 0)
	res, err := db.ClassSearch(ctx, traverser.GetParams{
		ClassName: class,
		Filters:   mergedFilter,
		Kind:      kind,
		Pagination: &libfilters.Pagination{
			Limit: 10000, // TODO: gh-1219 increase
		},
	})

	return res, err
}

// TODO: why is this logic in the persistence package? This is business-logic,
// move out of here!
func (db *DB) AggregateNeighbors(ctx context.Context, vector []float32,
	kind kind.Kind, class string, properties []string, k int,
	filter *libfilters.LocalFilter) ([]classification.NeighborRef, error) {

	mergedFilter := mergeUserFilterWithRefCountFilter(filter, class, properties,
		libfilters.OperatorGreaterThan, 0)
	res, err := db.VectorClassSearch(ctx, traverser.GetParams{
		Kind:         kind,
		ClassName:    class,
		SearchVector: vector,
		Pagination: &filters.Pagination{
			Limit: k,
		},
		Filters: mergedFilter,
	})
	if err != nil {
		return nil, errors.Wrap(err, "aggregate neighbors: search neighbors")
	}

	return NewKnnAggregator(res, vector).Aggregate(k, properties)
}

// TODO: this is business logic, move out of here
type KnnAggregator struct {
	input        search.Results
	sourceVector []float32
}

func NewKnnAggregator(input search.Results, sourceVector []float32) *KnnAggregator {
	return &KnnAggregator{input: input, sourceVector: sourceVector}
}

func (a *KnnAggregator) Aggregate(k int, properties []string) ([]classification.NeighborRef, error) {

	neighbors, err := a.extractBeacons(properties)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate: extract beacons from neighbors")
	}

	return a.aggregateBeacons(neighbors)
}

func (a *KnnAggregator) extractBeacons(properties []string) (neighborProps, error) {
	neighbors := neighborProps{}
	for i, elem := range a.input {
		schemaMap, ok := elem.Schema.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expecteded element[%d].Schema to be map, got: %T", i, elem.Schema)
		}

		for _, prop := range properties {
			refProp, ok := schemaMap[prop]
			if !ok {
				return nil, fmt.Errorf("expecteded element[%d].Schema to have property %q, but didn't", i, prop)
			}

			refTyped, ok := refProp.(models.MultipleRef)
			if !ok {
				return nil, fmt.Errorf("expecteded element[%d].Schema.%s to be models.MultipleRef, got: %T", i, prop, refProp)
			}

			if len(refTyped) != 1 {
				return nil, fmt.Errorf("expecteded element[%d].Schema.%s to have exactly one reference, got: %d",
					i, prop, len(refTyped))
			}

			distance, err := vectorizer.NormalizedDistance(a.sourceVector, elem.Vector)
			if err != nil {
				return nil, errors.Wrap(err, "calculate distance between source and candidate")
			}

			beacon := refTyped[0].Beacon.String()
			neighborProp := neighbors[prop]
			if neighborProp.beacons == nil {
				neighborProp.beacons = neighborBeacons{}
			}
			neighborProp.beacons[beacon] = append(neighborProp.beacons[beacon], distance)
			neighbors[prop] = neighborProp
		}
	}

	return neighbors, nil
}

func (a *KnnAggregator) aggregateBeacons(props neighborProps) ([]classification.NeighborRef, error) {
	var out []classification.NeighborRef
	for propName, prop := range props {
		var winningBeacon string
		var winningCount int

		for beacon, distances := range prop.beacons {
			if len(distances) > winningCount {
				winningBeacon = beacon
				winningCount = len(distances)
			}
		}

		winning, losing := a.calculateWinningAndLoosingDistances(prop.beacons, winningBeacon)

		out = append(out, classification.NeighborRef{
			Beacon:          strfmt.URI(winningBeacon),
			Count:           winningCount,
			Property:        propName,
			WinningDistance: winning,
			LosingDistance:  losing,
		})

	}

	return out, nil
}

func (a *KnnAggregator) calculateWinningAndLoosingDistances(beacons neighborBeacons, winner string) (float32, *float32) {
	var winningDistances []float32
	var losingDistances []float32

	for beacon, distances := range beacons {

		if beacon == winner {
			winningDistances = distances
		} else {
			losingDistances = append(losingDistances, distances...)
		}
	}

	var losingDistance *float32
	if len(losingDistances) > 0 {
		d := mean(losingDistances)
		losingDistance = &d
	}

	return mean(winningDistances), losingDistance
}

type neighborProps map[string]neighborProp

type neighborProp struct {
	beacons neighborBeacons
}

type neighborBeacons map[string][]float32

func mergeUserFilterWithRefCountFilter(userFilter *libfilters.LocalFilter, className string,
	properties []string, op libfilters.Operator, refCount int) *libfilters.LocalFilter {
	countFilters := make([]libfilters.Clause, len(properties))
	for i, prop := range properties {
		countFilters[i] = libfilters.Clause{
			Operator: op,
			Value: &libfilters.Value{
				Type:  schema.DataTypeInt,
				Value: refCount,
			},
			On: &libfilters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(prop),
			},
		}
	}

	var countRootClause libfilters.Clause
	if len(countFilters) == 1 {
		countRootClause = countFilters[0]
	} else {
		countRootClause = libfilters.Clause{
			Operands: countFilters,
			Operator: libfilters.OperatorAnd,
		}
	}

	var rootFilter = &libfilters.LocalFilter{}
	if userFilter == nil {
		rootFilter.Root = &countRootClause
	} else {
		rootFilter.Root = &libfilters.Clause{
			Operator: libfilters.OperatorAnd, // so we can AND the refcount requirements and whatever custom filters, the user has
			Operands: []libfilters.Clause{*userFilter.Root, countRootClause},
		}
	}

	return rootFilter
}

func mean(in []float32) float32 {
	sum := float32(0)
	for _, v := range in {
		sum += v
	}

	return sum / float32(len(in))
}
