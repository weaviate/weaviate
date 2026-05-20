//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package traverser

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_Traverser_Aggregate(t *testing.T) {
	principal := &models.Principal{}
	logger, _ := test.NewNullLogger()
	authorizer := mocks.NewMockAuthorizer()
	vectorRepo := &fakeVectorRepo{}
	explorer := &fakeExplorer{}
	schemaGetter := &fakeSchemaGetter{aggregateTestSchema}

	traverser := NewTraverser(&config.WeaviateConfig{}, logger, authorizer,
		vectorRepo, explorer, schemaGetter, nil, nil, -1)

	t.Run("with aggregation only", func(t *testing.T) {
		params := aggregation.Params{
			ClassName: "MyClass",
			Properties: []aggregation.ParamProperty{
				{
					Name:        "label",
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)},
				},
				{
					Name:        "number",
					Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				},
				{
					Name:        "int",
					Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				},
				{
					Name:        "date",
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)},
				},
			},
		}

		agg := aggregation.Result{
			Groups: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"label": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Foo",
										Occurs: 200,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"date": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Bar",
										Occurs: 100,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"number": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 200,
							},
						},
						"int": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 100,
							},
						},
					},
				},
			},
		}

		vectorRepo.On("Aggregate", params).Return(&agg, nil)
		res, err := traverser.Aggregate(context.Background(), principal, &params)
		require.Nil(t, err)
		assert.Equal(t, &agg, res)
	})

	t.Run("with a mix of aggregation and type inspection", func(t *testing.T) {
		params := aggregation.Params{
			ClassName: "MyClass",
			Properties: []aggregation.ParamProperty{
				{
					Name: "label",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.NewTopOccurrencesAggregator(nil),
					},
				},
				{
					Name: "number",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.SumAggregator,
					},
				},
				{
					Name: "int",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.SumAggregator,
					},
				},
				{
					Name: "date",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.NewTopOccurrencesAggregator(nil),
					},
				},
				{
					Name:        "a ref",
					Aggregators: []aggregation.Aggregator{aggregation.TypeAggregator},
				},
			},
		}

		agg := aggregation.Result{
			Groups: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"label": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Foo",
										Occurs: 200,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"date": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Bar",
										Occurs: 100,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"number": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 200,
							},
						},
						"int": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 100,
							},
						},
					},
				},
			},
		}

		expectedResult := aggregation.Result{
			Groups: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"label": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Foo",
										Occurs: 200,
									},
								},
							},
							Type:       aggregation.PropertyTypeText,
							SchemaType: string(schema.DataTypeText),
						},
						"date": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Bar",
										Occurs: 100,
									},
								},
							},
							SchemaType: string(schema.DataTypeDate),
							Type:       aggregation.PropertyTypeText,
						},
						"number": {
							Type:       aggregation.PropertyTypeNumerical,
							SchemaType: string(schema.DataTypeNumber),
							NumericalAggregations: map[string]interface{}{
								"sum": 200,
							},
						},
						"int": {
							Type:       aggregation.PropertyTypeNumerical,
							SchemaType: string(schema.DataTypeInt),
							NumericalAggregations: map[string]interface{}{
								"sum": 100,
							},
						},
						"a ref": {
							Type: aggregation.PropertyTypeReference,
							ReferenceAggregation: aggregation.Reference{
								PointingTo: []string{"AnotherClass"},
							},
							SchemaType: string(schema.DataTypeCRef),
						},
					},
				},
			},
		}

		vectorRepo.On("Aggregate", params).Return(&agg, nil)
		res, err := traverser.Aggregate(context.Background(), principal, &params)
		require.Nil(t, err)
		assert.Equal(t, &expectedResult, res)
	})

	t.Run("with hybrid search", func(t *testing.T) {
		params := aggregation.Params{
			ClassName: "MyClass",
			Properties: []aggregation.ParamProperty{
				{
					Name:        "label",
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)},
				},
				{
					Name:        "number",
					Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				},
				{
					Name:        "int",
					Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				},
				{
					Name:        "date",
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(nil)},
				},
			},
			IncludeMetaCount: true,
			Hybrid: &searchparams.HybridSearch{
				Type:   "hybrid",
				Alpha:  0.5,
				Query:  "some query",
				Vector: []float32{1, 2, 3},
			},
		}

		agg := aggregation.Result{
			Groups: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"label": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Foo",
										Occurs: 200,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"date": {
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "Bar",
										Occurs: 100,
									},
								},
							},
							Type: aggregation.PropertyTypeText,
						},
						"number": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 200,
							},
						},
						"int": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 100,
							},
						},
					},
				},
			},
		}

		vectorRepo.On("Aggregate", params).Return(&agg, nil)
		res, err := traverser.Aggregate(context.Background(), principal, &params)
		require.Nil(t, err)
		assert.Equal(t, &agg, res)
	})

	t.Run("with hybrid search and multiple target vectors", func(t *testing.T) {
		params := aggregation.Params{
			ClassName: "MyClassNamedVectors",
			Properties: []aggregation.ParamProperty{
				{
					Name:        "number",
					Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				},
			},
			IncludeMetaCount: true,
			Hybrid: &searchparams.HybridSearch{
				Type:          "hybrid",
				Alpha:         0.5,
				Query:         "some query",
				Vector:        []float32{1, 2, 3},
				TargetVectors: []string{"title_vec", "body_vec"},
			},
		}

		expectedParams := params
		expectedParams.TargetVector = "title_vec"

		agg := aggregation.Result{
			Groups: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"number": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"sum": 200,
							},
						},
					},
				},
			},
		}

		vectorRepo.On("Aggregate", expectedParams).Return(&agg, nil).Once()
		res, err := traverser.Aggregate(context.Background(), principal, &params)
		require.NoError(t, err)
		assert.Equal(t, "title_vec", params.TargetVector)
		assert.Equal(t, []string{"title_vec", "body_vec"}, params.Hybrid.TargetVectors)
		assert.Equal(t, &agg, res)
	})
}

var aggregateTestSchema = schema.Schema{
	Objects: &models.Schema{
		Classes: []*models.Class{
			{
				Class: "AnotherClass",
			},
			{
				Class: "MyClass",
				Properties: []*models.Property{
					{
						Name:         "label",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
					{
						Name:     "number",
						DataType: []string{string(schema.DataTypeNumber)},
					},
					{
						Name:     "int",
						DataType: []string{string(schema.DataTypeInt)},
					},
					{
						Name:     "date",
						DataType: []string{string(schema.DataTypeDate)},
					},
					{
						Name:     "a ref",
						DataType: []string{"AnotherClass"},
					},
				},
			},
			{
				Class: "MyClassNamedVectors",
				Properties: []*models.Property{
					{
						Name:     "number",
						DataType: []string{string(schema.DataTypeInt)},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"title_vec": {
						Vectorizer: map[string]interface{}{
							"none": map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
					"body_vec": {
						Vectorizer: map[string]interface{}{
							"none": map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
				},
			},
		},
	},
}
