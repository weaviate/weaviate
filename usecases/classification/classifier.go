package classification

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type Classifier struct {
	schemaGetter schemaUC.SchemaGetter
	repo         Repo
	vectorRepo   VectorRepo
}

func New(sg schemaUC.SchemaGetter, cr Repo, vr VectorRepo) *Classifier {
	return &Classifier{
		schemaGetter: sg,
		repo:         cr,
		vectorRepo:   vr,
	}
}

// Repo to manage classification state, should be consistent, not used to store
// acutal data object vectors, see VectorRepo
type Repo interface {
	Put(models.Classification) error
	Get(id strfmt.UUID) (*models.Classification, error)
}

type VectorRepo interface {
	GetUnclassified(ctx context.Context, class string, properites []string) (*search.Results, error)
	AggregateNeighbors(ctx context.Context, vector []float32, class string,
		properties []string, k int) ([]NeighborRef, error)
	PutThing(ctx context.Context, thing *models.Thing, vector []float32) error
	PutAction(ctx context.Context, action *models.Action, vector []float32) error
}

// NeighborRef is the result of an aggregation of the ref properties of k neighbors
type NeighborRef struct {
	// Property indicates which property was aggregated
	Property string

	// The beacon of the most common (kNN) reference
	Beacon strfmt.URI

	// Count (n<=k) of number of the winning Beacon
	Count int
}

func (c *Classifier) Schedule(params models.Classification) (*models.Classification, error) {
	err := NewValidator(c.schemaGetter, params).Do()
	if err != nil {
		return nil, err
	}

	if params.K == nil {
		defaultK := int32(3)
		params.K = &defaultK
	}

	if err := c.assignNewID(&params); err != nil {
		return nil, fmt.Errorf("classification: assign id: %v", err)
	}

	params.Status = models.ClassificationStatusRunning

	if err := c.repo.Put(params); err != nil {
		return nil, fmt.Errorf("classification: put: %v", err)
	}

	// asynchronously trigger the classification
	go c.run(params)

	return &params, nil
}

func (c *Classifier) assignNewID(params *models.Classification) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	params.ID = strfmt.UUID(id.String())
	return nil
}

func (c *Classifier) Get(id strfmt.UUID) (*models.Classification, error) {
	return c.repo.Get(id)
}
