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

package test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

// appleVec is the t2v-contextionary representation of "Apple Inc."
var appleVec = []float32{
	0.1156649, -0.3561866, 0.4718789, 0.37318036, 0.39549947, 0.019409189, -0.5052104, -0.49448758,
	0.34452468, 0.46354344, 0.1932035, 0.51334095, 0.06032639, 0.022086846, 0.20391269, 0.3013975,
	0.18838425, -0.2362212, -0.25797912, -0.11189923, -0.14507815, 0.3113891, -0.90078014, 0.027230136,
	-0.5541761, -0.33453932, 0.9467, 0.39270592, 0.0775289, -0.14601035, -0.5497628, 0.34385568,
	0.5363504, 0.03164669, 0.03510879, -0.37564012, 0.22805381, -0.66345274, -0.92397606, 0.85855925,
	-0.5637805, 0.035184387, 0.23299722, -0.042199645, -0.52195567, -0.17418303, -0.029039165, 0.4399605,
	0.36524323, 0.21769615, -0.1977588, -0.17114285, 0.30731055, -0.6743735, 0.25451374, 0.41582933,
	0.61602086, 0.3382223, 0.39701316, -0.54065305, -0.16107371, -0.80420196, -0.42476287, 0.40522298,
	-0.24763498, -0.7224363, -0.5512907, -0.0400732, -0.09994836, -0.2354202, 0.2904534, -0.12089672,
	-0.07095274, -0.8213324, 0.3695029, 0.27129403, 0.28678897, -0.108535565, -0.30699188, 0.10705576,
	0.08372605, -0.64183795, 0.34861454, -0.30277634, 0.21602349, -0.23038381, -0.10144254, -0.47548878,
	0.3525676, 0.3357812, 0.031383604, -0.32346088, -0.7515443, -0.14595662, -0.1425658, 0.54312915,
	-0.60661954, 0.10959545, -0.17200017, 0.60667217, -0.22193804, 0.5861486, 0.4714104, -0.4168524,
	-0.23929326, 0.47505698, -0.5256647, 0.23308091, 0.16735256, -0.021147087, -0.6238067, -0.065388694,
	0.38134024, 0.17625189, -0.048189547, -0.40676376, -0.20627557, -0.6200684, 0.24607961, -0.7479579,
	0.36243674, -0.41451588, -0.3258561, 0.07216902, 0.15214325, 0.2363326, 1.7854439, 0.2354896,
	-0.80430084, 0.39550564, 0.06727363, 0.45679152, 0.09223966, -0.17635022, 0.065364204, -0.6799169,
	0.46794528, -0.6863512, -0.007789179, 0.0216118, 0.3218315, -0.329095, -0.15101263, 0.054294955,
	0.35598493, 0.8095643, 0.4240984, 0.107904576, -0.65505075, -0.25601476, -0.040415946, 0.57646215,
	-0.14216466, -0.5626221, 0.21731018, 0.25857863, 0.029463748, -0.043640777, -0.86262965, 0.0075217593,
	-0.65511745, 0.30682194, 0.36109644, -0.34552526, -0.57620883, -0.111058705, 0.42360848, 0.22977945,
	0.058191486, -0.6967789, -0.083894424, 0.21894856, -0.15210733, 0.2840013, -0.66721946, -0.12251554,
	-0.55239767, -0.06489324, -0.17015795, -0.15400846, 0.14791602, -0.76380575, 0.27046034, -0.47688308,
	0.25788718, -0.074898824, 0.181136, 0.6860475, -0.14676934, 0.13610536, 0.74407804, -0.26433572,
	-0.09919782, -0.26012585, -0.18844572, 0.8116442, 0.24614683, 0.076953486, 0.41485175, -0.64702696,
	-0.5514351, -0.44831908, 0.7871427, 0.1256176, -0.37650946, 0.26002303, 0.55952126, -0.5275842,
	0.7185946, 0.09147637, -0.3937243, 0.10171145, -0.6451931, 0.8872601, 0.011252741, 1.1493335,
	0.7991122, -0.16108659, -0.7322848, 0.5237607, -0.50677204, 0.12007416, -0.6966177, -0.5039344,
	0.020131318, 0.15328859, -1.0066653, 0.32302102, -0.36504102, 0.37823763, -0.19183074, -0.4154492,
	0.14257756, 0.6225165, -0.24297066, 0.014472419, 0.8159169, 1.2461865, 0.07883369, -0.35416773,
	-0.06593153, -0.81301326, 0.17566697, -0.04062626, -0.112336636, -0.22738501, -0.42422646, 0.458409,
	0.79599, 0.33880755, 0.39182758, 0.054381482, 0.5805471, 0.25382927, -0.16633242, 0.08435115,
	0.53753984, -0.16825016, -0.69669664, 0.21506411, -0.35470957, 0.25212923, 0.20211501, 0.6161077,
	-0.077442676, -0.024064686, -0.18163882, 0.6834761, -1.0793741, 0.25927436, -0.69374615, -0.025031673,
	-0.1307808, -0.5026866, -0.14586367, -0.41198593, -0.4018977, 0.10252101, -0.22274522, 0.9635526,
	-0.17163973, 0.1639396, 0.66181034, -0.42865846, -0.18711954, -0.23968346, -0.09696686, 0.38911402,
	0.0962325, 0.46173036, 0.10814153, 1.0249863, -0.2061986, 0.6657442, -0.3277397, 0.26586995,
	-0.12981872, 0.40097368, -0.49962977, -0.61136127,
}

func getWithHybridSearch(t *testing.T) {
	t.Run("without references", func(t *testing.T) {
		query := `
		{
  			Get {
    			Airport
    			(
      				hybrid: {
        				alpha: 0
        				query: "10000"
      				}
				)
    			{
      				code
    			}
  			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Airport").AsSlice()
		require.Len(t, result, 1)
		assert.EqualValues(t, map[string]interface{}{"code": "10000"}, result[0])
	})

	t.Run("with limit and vector", func(t *testing.T) {
		limit := 2
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					limit: %d
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
						vector: %s
					}
				) {
					name
				}
			}
		}`, limit, graphqlhelper.Vec2String(appleVec))
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, limit)
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple",
		})
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple Inc.",
		})
	})

	t.Run("with limit and no vector", func(t *testing.T) {
		limit := 2
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					limit: %d
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					name
				}
			}
		}`, limit)
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, limit)
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple",
		})
		assert.Contains(t, result, map[string]interface{}{
			"name": "Apple Inc.",
		})
	})

	t.Run("with no limit and vector", func(t *testing.T) {
		query := fmt.Sprintf(`
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
						vector: %s
					}
				) {
					name
				}
			}
		}`, graphqlhelper.Vec2String(appleVec))
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with no limit and no vector", func(t *testing.T) {
		query := `
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					name
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
	})

	t.Run("with _additional{vector}", func(t *testing.T) {
		query := `
		{
		  	Get {
				Company(
					hybrid: {
						query: "Apple", 
						alpha: 0.5, 
					}
				) {
					_additional {
						vector
					}
				}
			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Company").AsSlice()
		require.Len(t, result, 9)
		for _, res := range result {
			company := res.(map[string]interface{})
			addl := company["_additional"].(map[string]interface{})
			vec, found := addl["vector"]
			assert.True(t, found)
			assert.Len(t, vec, 300)
		}
	})

	t.Run("with references", func(t *testing.T) {
		query := `
		{
  			Get {
    			Airport
    			(
      				hybrid: {
        				alpha: 0.5
        				query: "1000"
      				}
				)
    			{
      				code
      				inCity {
        				... on City {
          					name
        				}
      				}
    			}
  			}
		}`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query).Get("Get", "Airport").AsSlice()
		require.Len(t, result, 4)
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "10000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Amsterdam"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "20000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Rotterdam"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "30000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Dusseldorf"},
				},
			})
		assert.Contains(t, result,
			map[string]interface{}{
				"code": "40000",
				"inCity": []interface{}{
					map[string]interface{}{"name": "Berlin"},
				},
			})
	})
}
