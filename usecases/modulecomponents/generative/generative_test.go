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

package generative

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_MakeTaskPrompt(t *testing.T) {
	prompt, err := MakeTaskPrompt([]map[string]string{{"title": "A Grand Day Out"}}, "Create a story based on the following properties")
	require.Nil(t, err)
	require.Equal(t, "Create a story based on the following properties: [{\"title\":\"A Grand Day Out\"}]", prompt)
}

func Test_MakeSinglePrompt(t *testing.T) {
	prompt, err := MakeSinglePrompt(map[string]string{"title": "A Grand Day Out"}, "Create a story based on \"{title}\"")
	require.Nil(t, err)
	require.Equal(t, "Create a story based on \"A Grand Day Out\"", prompt)
}

func Test_ParseImageProperties(t *testing.T) {
	earth, mars := "earth-base64", "mars-base64"

	tests := []struct {
		name           string
		inputImages    []*string
		propertyNames  []string
		storedImages   []map[string]*string
		expectedImages []*string
	}{
		{
			name:           "no images at all",
			expectedImages: []*string{},
		},
		{
			name:           "user provided images only",
			inputImages:    []*string{&earth},
			expectedImages: []*string{&earth},
		},
		{
			name:           "stored image resolved by property name",
			propertyNames:  []string{"image"},
			storedImages:   []map[string]*string{{"image": &earth}},
			expectedImages: []*string{&earth},
		},
		{
			name:           "requested property is not present on the object",
			propertyNames:  []string{"thumbnail"},
			storedImages:   []map[string]*string{{"image": &earth}},
			expectedImages: []*string{},
		},
		{
			name:           "one of several objects is missing the property",
			propertyNames:  []string{"image"},
			storedImages:   []map[string]*string{{"image": &earth}, {"other": &mars}},
			expectedImages: []*string{&earth},
		},
		{
			name:           "stored images come before user provided ones",
			inputImages:    []*string{&mars},
			propertyNames:  []string{"image"},
			storedImages:   []map[string]*string{{"image": &earth}},
			expectedImages: []*string{&earth, &mars},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			images := ParseImageProperties(tt.inputImages, tt.propertyNames, tt.storedImages)
			require.Equal(t, tt.expectedImages, images)
			for i := range images {
				require.NotNil(t, images[i], "image at index %d must not be nil", i)
			}
		})
	}
}
