//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grouper

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/vectorizer"
)

// Grouper groups or merges search results by how related they are
type Grouper struct {
	logger logrus.FieldLogger
}

// NewGrouper creates a Grouper UC from the specified configuration
func New(logger logrus.FieldLogger) *Grouper {
	return &Grouper{logger: logger}
}

// Group using the applied strategy and force
func (g *Grouper) Group(in []search.Result, strategy string,
	force float32,
) ([]search.Result, error) {
	groups := groups{logger: g.logger}

	for _, current := range in {
		pos, ok := groups.hasMatch(current.Vector, force)
		if !ok {
			groups.new(current)
		} else {
			groups.Elements[pos].add(current)
		}
	}

	return groups.flatten(strategy)
}

type group struct {
	Elements []search.Result `json:"elements"`
}

func (g *group) add(item search.Result) {
	g.Elements = append(g.Elements, item)
}

func (g group) matches(vector []float32, force float32) bool {
	// iterate over all group Elements and consider it a match if any matches
	for _, elem := range g.Elements {
		dist, err := vectorizer.NormalizedDistance(vector, elem.Vector)
		if err != nil {
			// TODO: log error
			// we don't expect to ever see this error, so we don't need to handle it
			// explicitly, however, let's still log it in case that the above
			// assumption is wrong
			continue
		}

		if dist < force {
			return true
		}
	}

	return false
}

type groups struct {
	Elements []group `json:"elements"`
	logger   logrus.FieldLogger
}

func (gs groups) hasMatch(vector []float32, force float32) (int, bool) {
	for pos, group := range gs.Elements {
		if group.matches(vector, force) {
			return pos, true
		}
	}
	return -1, false
}

func (gs *groups) new(item search.Result) {
	gs.Elements = append(gs.Elements, group{Elements: []search.Result{item}})
}

func (gs groups) flatten(strategy string) (out []search.Result, err error) {
	gs.logger.WithField("object", "grouping_before_flatten").
		WithField("strategy", strategy).
		WithField("groups", gs.Elements).
		Debug("group before flattening")

	switch strategy {
	case "closest":
		out, err = gs.flattenClosest()
	case "merge":
		out, err = gs.flattenMerge()
	default:
		return nil, fmt.Errorf("unrecognized grouping strategy '%s'", strategy)
	}
	if err != nil {
		return
	}

	gs.logger.WithField("object", "grouping_after_flatten").
		WithField("strategy", strategy).
		WithField("groups", gs.Elements).
		Debug("group after flattening")

	return out, nil
}

func (gs groups) flattenClosest() ([]search.Result, error) {
	out := make([]search.Result, len(gs.Elements))
	for i, group := range gs.Elements {
		out[i] = group.Elements[0] // hard-code "closest" strategy for now
	}

	return out, nil
}

func (gs groups) flattenMerge() ([]search.Result, error) {
	out := make([]search.Result, len(gs.Elements))
	for i, group := range gs.Elements {
		merged, err := group.flattenMerge()
		if err != nil {
			return nil, fmt.Errorf("group %d: %v", i, err)
		}

		out[i] = merged
	}

	return out, nil
}
