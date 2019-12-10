package grouper

import (
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
	"github.com/sirupsen/logrus"
)

// Grouper groups or merges search results by how releated they are
type Grouper struct {
	logger logrus.FieldLogger
}

// NewGrouper creates a Grouper UC from the specified configuration
func New(logger logrus.FieldLogger) *Grouper {
	return &Grouper{logger: logger}
}

// Group using the applied strategy and force
func (g *Grouper) Group(in []search.Result, strategy string,
	force float32) ([]search.Result, error) {

	var groups = groups{logger: g.logger}

	for _, current := range in {
		pos, ok := groups.hasMatch(current.Vector, force)
		if !ok {
			groups.new(current)
		} else {
			groups.Elements[pos].add(current)
		}
	}

	return groups.flatten(strategy), nil
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

func (gs groups) flatten(strategy string) []search.Result {
	gs.logger.WithField("action", "grouping_before_flatten").
		WithField("strategy", strategy).
		WithField("groups", gs.Elements).
		Debug("group before flattening")

	out := make([]search.Result, len(gs.Elements), len(gs.Elements))
	for i, group := range gs.Elements {
		out[i] = group.Elements[0] // hard-code "closest" strategy for now
	}

	gs.logger.WithField("action", "grouping_after_flatten").
		WithField("strategy", strategy).
		WithField("groups", gs.Elements).
		Debug("group after flattening")

	return out
}
