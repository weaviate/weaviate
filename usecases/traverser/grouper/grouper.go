package grouper

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
)

// Grouper groups or merges search results by how releated they are
type Grouper struct{}

// NewGrouper creates a Grouper UC from the specified configuration
func New() *Grouper {
	return &Grouper{}
}

// Group using the applied strategy and force
func (g *Grouper) Group(in []search.Result, strategy string,
	force float32) ([]search.Result, error) {

	var groups groups

	for _, current := range in {
		pos, ok := groups.hasMatch(current.Vector, force)
		if !ok {
			groups.new(current)
		} else {
			groups.elements[pos].add(current)
		}
	}

	return groups.flatten(strategy), nil
}

type group struct {
	elements []search.Result
}

func (g group) add(item search.Result) {
	g.elements = append(g.elements, item)
}

func (g group) matches(vector []float32, force float32) bool {
	// iterate over all group elements and consider it a match if any matches
	for _, elem := range g.elements {
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
	elements []group
}

func (gs groups) hasMatch(vector []float32, force float32) (int, bool) {
	for pos, group := range gs.elements {
		if group.matches(vector, force) {
			return pos, true
		}
	}
	return -1, false
}

func (gs *groups) new(item search.Result) {
	gs.elements = append(gs.elements, group{elements: []search.Result{item}})
}

func (gs groups) flatten(strategy string) []search.Result {
	spew.Dump(gs.elements)
	out := make([]search.Result, len(gs.elements), len(gs.elements))
	for i, group := range gs.elements {
		out[i] = group.elements[0] // hard-code "closest" strategy for now
	}

	return out
}
