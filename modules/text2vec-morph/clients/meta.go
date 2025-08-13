//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

func (v *client) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Morph Module",
		"documentationHref": "https://docs.morphllm.com/api-reference/endpoint/embedding",
	}, nil
}
