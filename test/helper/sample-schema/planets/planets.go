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

package planets

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

var Planets = []struct {
	ID                strfmt.UUID
	Name, Description string
}{
	{
		ID:   strfmt.UUID("00000000-0000-0000-0000-000000000001"),
		Name: "Earth",
		Description: `
		The Earth's surface is predominantly covered by oceans, accounting for about 71% of its total area, while continents provide 
		the stage for bustling cities, towering mountains, and sprawling forests. Its atmosphere, composed mostly of nitrogen and oxygen, 
		protects life from harmful solar radiation and regulates the planet's climate, creating the conditions necessary for life to flourish.

		Humans, as the dominant species, have left an indelible mark on Earth, shaping its landscapes and ecosystems in profound ways. 
		However, with this influence comes the responsibility to steward and preserve our planet for future generations.
		`,
	},
	{
		ID:   strfmt.UUID("00000000-0000-0000-0000-000000000002"),
		Name: "Mars",
		Description: `
		Mars, often called the "Red Planet" due to its rusty reddish hue, is the fourth planet from the Sun in our solar system. 
		It's a world of stark contrasts and mysterious allure, captivating the imaginations of scientists, explorers, and dreamers alike.

		With its barren, rocky terrain and thin atmosphere primarily composed of carbon dioxide, Mars presents a harsh environment vastly 
		different from Earth. Yet, beneath its desolate surface lie tantalizing clues about its past, including evidence of ancient rivers, 
		lakes, and even the possibility of microbial life.
		`,
	},
}

func BaseClass(className string) *models.Class {
	return &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name: "name", DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: "description", DataType: []string{schema.DataTypeText.String()},
			},
		},
	}
}

func InsertObjects(t *testing.T, className string) {
	for _, company := range Planets {
		obj := &models.Object{
			Class: className,
			ID:    company.ID,
			Properties: map[string]interface{}{
				"name":        company.Name,
				"description": company.Description,
			},
		}
		helper.CreateObject(t, obj)
		helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
	}
}

func CreateTweetTest(t *testing.T, className string) {
	CreateTweetTestWithParams(t, className, "")
}

func CreateTweetTestWithParams(t *testing.T, className, params string) {
	prompt := "Write a short tweet about planet {name}"
	query := fmt.Sprintf(`
			{
				Get {
					%s{
						name
						_additional {
							generate(
								singleResult: {
									prompt: """
										%s
									"""
									%s
								}
							) {
								singleResult
								error
							}
						}
					}
				}
			}
		`, className, prompt, params)
	result := graphqlhelper.AssertGraphQLWithTimeout(t, helper.RootAuth, 5*time.Minute, query)
	objs := result.Get("Get", className).AsSlice()
	require.Len(t, objs, 2)
	for _, obj := range objs {
		name := obj.(map[string]interface{})["name"]
		assert.NotEmpty(t, name)
		additional, ok := obj.(map[string]interface{})["_additional"].(map[string]interface{})
		require.True(t, ok)
		require.NotNil(t, additional)
		generate, ok := additional["generate"].(map[string]interface{})
		require.True(t, ok)
		require.NotNil(t, generate)
		require.Nil(t, generate["error"])
		require.NotNil(t, generate["singleResult"])
		singleResult, ok := generate["singleResult"].(string)
		require.True(t, ok)
		require.NotEmpty(t, singleResult)
		// print the results of the prompt
		t.Logf("Prompt: %s\nResult: %s\n", prompt, singleResult)
	}
}
