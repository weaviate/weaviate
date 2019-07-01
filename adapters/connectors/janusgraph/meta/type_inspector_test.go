/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
 * LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
 * CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package meta

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_TypeInspector_WithReferenceProp(t *testing.T) {
	t.Run("when the user askes for 'pointingTo'", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"InCountry": map[string]interface{}{
				"pointingTo": []interface{}{
					"Country", "WeaviateB/Country",
				},
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})

	t.Run("when the user askes for type", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Type},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"InCountry": map[string]interface{}{
				"type": "cref",
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})

	t.Run("when the user asks for both 'type' and 'pointingTo'", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo, kinds.Type},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"InCountry": map[string]interface{}{
				"pointingTo": []interface{}{
					"Country", "WeaviateB/Country",
				},
				"type": "cref",
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})

	t.Run("when the user askes for both 'count' and 'pointingTo'", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count, kinds.PointingTo},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"InCountry": map[string]interface{}{
				"pointingTo": []interface{}{
					"Country", "WeaviateB/Country",
				},
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})

	t.Run("when the user askes for unrelated statisticals props (count)", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
		}

		expectedOutput := map[string]interface{}{}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "it should skip over this particular type")
	})
}

func Test_TypeInspector_WithoutProperties(t *testing.T) {
	input := kinds.GetMetaParams{
		ClassName:  schema.ClassName("City"),
		Properties: []kinds.MetaProperty{},
	}

	expectedOutput := map[string]interface{}{}

	result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
	require.Nil(t, err, "should not error")

	assert.Equal(t, expectedOutput, result, "should produce an empty map, but also not error")
}

func Test_TypeInspector_WithMetaProperties(t *testing.T) {
	input := kinds.GetMetaParams{
		ClassName: schema.ClassName("City"),
		Properties: []kinds.MetaProperty{
			kinds.MetaProperty{
				Name:                "meta",
				StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
			},
		},
	}

	expectedOutput := map[string]interface{}{}

	result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
	require.Nil(t, err, "should not error")

	assert.Equal(t, expectedOutput, result, "should produce an empty map, but also not error")
}

func Test_TypeInspector_WithPrimitiveProps(t *testing.T) {
	t.Run("on an int with only 'type'", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "population",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Type},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"population": map[string]interface{}{
				"type": "int",
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})

	t.Run("on an int with 'type' and other statistical analyses", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "population",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Mean, kinds.Type, kinds.Count},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"population": map[string]interface{}{
				"type": "int",
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})
}

func Test_TypeInspector_WithMultiplePropsOfDifferentTypes(t *testing.T) {
	t.Run("with mixed prop types and mixed statistical analysis types", func(t *testing.T) {
		input := kinds.GetMetaParams{
			ClassName: schema.ClassName("City"),
			Properties: []kinds.MetaProperty{
				kinds.MetaProperty{
					Name:                "InCountry",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo, kinds.Count},
				},
				kinds.MetaProperty{
					Name:                "population",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Mean, kinds.Type, kinds.Count},
				},
			},
		}

		expectedOutput := map[string]interface{}{
			"population": map[string]interface{}{
				"type": "int",
			},
			"InCountry": map[string]interface{}{
				"pointingTo": []interface{}{
					"Country", "WeaviateB/Country",
				},
			},
		}

		result, err := NewTypeInspector(&fakeTypeSource{}).Process(&input)
		require.Nil(t, err, "should not error")

		assert.Equal(t, expectedOutput, result, "should extract the types correctly")
	})
}
