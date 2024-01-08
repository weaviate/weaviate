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
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_Traverser_Aggregate(t *testing.T) {
	principal := &models.Principal{}
	logger, _ := test.NewNullLogger()
	locks := &fakeLocks{}
	authorizer := &fakeAuthorizer{}
	vectorRepo := &fakeVectorRepo{}
	explorer := &fakeExplorer{}
	schemaGetter := &fakeSchemaGetter{aggregateTestSchema}

	traverser := NewTraverser(&config.WeaviateConfig{}, locks, logger, authorizer,
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
		t.Logf("res: %+v", res)
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
		},
	},
}
