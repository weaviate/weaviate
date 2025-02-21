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
