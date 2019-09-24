//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package classification

import (
	"context"
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	uuid "github.com/satori/go.uuid"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	schemaUC "github.com/semi-technologies/weaviate/usecases/schema"
)

type Classifier struct {
	schemaGetter schemaUC.SchemaGetter
	repo         Repo
	vectorRepo   vectorRepo
}

func New(sg schemaUC.SchemaGetter, cr Repo, vr vectorRepo) *Classifier {
	return &Classifier{
		schemaGetter: sg,
		repo:         cr,
		vectorRepo:   vr,
	}
}

// Repo to manage classification state, should be consistent, not used to store
// acutal data object vectors, see VectorRepo
type Repo interface {
	Put(ctx context.Context, classification models.Classification) error
	Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error)
}

type VectorRepo interface {
	GetUnclassified(ctx context.Context, kind kind.Kind, class string, properites []string) ([]search.Result, error)
	AggregateNeighbors(ctx context.Context, vector []float32,
		kind kind.Kind, class string, properties []string, k int) ([]NeighborRef, error)
}

type vectorRepo interface {
	VectorRepo
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

func (c *Classifier) Schedule(ctx context.Context, params models.Classification) (*models.Classification, error) {
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
	params.Meta = &models.ClassificationMeta{
		Started: strfmt.DateTime(time.Now()),
	}

	if err := c.repo.Put(ctx, params); err != nil {
		return nil, fmt.Errorf("classification: put: %v", err)
	}

	// asynchronously trigger the classification
	kind := c.getKind(params)
	go c.run(params, kind)

	return &params, nil
}

func (c *Classifier) getKind(params models.Classification) kind.Kind {
	s := c.schemaGetter.GetSchemaSkipAuth()
	kind, _ := s.GetKindOfClass(schema.ClassName(params.Class))
	// skip nil-check as we have made it past validation
	return kind
}

func (c *Classifier) assignNewID(params *models.Classification) error {
	id, err := uuid.NewV4()
	if err != nil {
		return err
	}

	params.ID = strfmt.UUID(id.String())
	return nil
}

func (c *Classifier) Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error) {
	return c.repo.Get(ctx, id)
}
