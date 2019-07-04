package traverser

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// Explorer is a helper construct to perform vector-based searches. It does not
// contain monitoring or authorization checks. It should thus never be directly
// used by an API, but through a Traverser.
type Explorer struct {
	search     vectorClassSearch
	vectorizer corpiVectorizer
	repo       explorerRepo
}

type vectorClassSearch interface {
	VectorClassSearch(ctx context.Context, kind kind.Kind,
		className string, vector []float32, limit int,
		filters *filters.LocalFilter) ([]VectorSearchResult, error)
}

type explorerRepo interface {
	// TODO gh-912 leave decision how to resolve IDs to connector
	GetThing(context.Context, strfmt.UUID, *models.Thing) error
	GetAction(context.Context, strfmt.UUID, *models.Action) error
}

// NewExplorer with search and connector repo
func NewExplorer(search vectorClassSearch, vectorizer corpiVectorizer,
	repo explorerRepo) *Explorer {
	return &Explorer{search, vectorizer, repo}
}

// GetClass from search and connector repo
func (e *Explorer) GetClass(ctx context.Context,
	params *LocalGetParams) ([]interface{}, error) {
	if params.Filters != nil {
		msg := "combining 'explore' and 'where' parameters not possible yet - coming soon!"
		// TODO: enable in gh-911
		return nil, fmt.Errorf(msg)
	}

	searchVector, err := e.vectorFromExploreParams(ctx, params.Explore)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vectorize params: %v", err)
	}

	// TODO: gh-881 default to config limit
	limit := 100
	if params.Pagination != nil {
		limit = params.Pagination.Limit
	}

	res, err := e.search.VectorClassSearch(ctx, params.Kind, params.ClassName,
		searchVector, limit, params.Filters)
	if err != nil {
		return nil, fmt.Errorf("explorer: get class: vector search: %v", err)
	}

	return e.searchResultsToGetResponse(ctx, res)
}

func (e *Explorer) searchResultsToGetResponse(ctx context.Context,
	input []VectorSearchResult) ([]interface{}, error) {
	output := make([]interface{}, len(input), len(input))

	for i, res := range input {
		switch res.Kind {
		case kind.Thing:
			var thing models.Thing
			e.repo.GetThing(ctx, res.ID, &thing)
			output[i] = thing.Schema
		case kind.Action:
			var action models.Action
			e.repo.GetAction(ctx, res.ID, &action)
			output[i] = action.Schema
		default:
			return nil, fmt.Errorf("impossible kind %v", res.Kind)
		}
	}

	return output, nil
}

// TODO gh-881: incorporate duplicate from traverser
func (e *Explorer) vectorFromExploreParams(ctx context.Context,
	params *ExploreParams) ([]float32, error) {

	vector, err := e.vectorizer.Corpi(ctx, params.Values)
	if err != nil {
		return nil, fmt.Errorf("vectorize keywords: %v", err)
	}

	if params.MoveTo.Force > 0 && len(params.MoveTo.Values) > 0 {
		moveToVector, err := e.vectorizer.Corpi(ctx, params.MoveTo.Values)
		if err != nil {
			return nil, fmt.Errorf("vectorize move to: %v", err)
		}

		afterMoveTo, err := e.vectorizer.MoveTo(vector, moveToVector, params.MoveTo.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveTo
	}

	if params.MoveAwayFrom.Force > 0 && len(params.MoveAwayFrom.Values) > 0 {
		moveAwayVector, err := e.vectorizer.Corpi(ctx, params.MoveAwayFrom.Values)
		if err != nil {
			return nil, fmt.Errorf("vectorize move away from: %v", err)
		}

		afterMoveFrom, err := e.vectorizer.MoveAwayFrom(vector, moveAwayVector,
			params.MoveAwayFrom.Force)
		if err != nil {
			return nil, err
		}
		vector = afterMoveFrom
	}

	return vector, nil
}
