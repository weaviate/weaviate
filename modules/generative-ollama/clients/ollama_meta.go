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

package ollama

func (v *ollama) MetaInfo() (map[string]interface{}, error) {
	// Or this could be attached to the container
	// Needs to be to get the model path
	return map[string]interface{}{
		"name":              "Generative Search - Ollama",
		"documentationHref": "https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-completion",
	}, nil
}
