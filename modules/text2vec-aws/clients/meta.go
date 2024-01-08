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

package clients

func (v *aws) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "AWS Module",
		"documentationHref": "https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings",
	}, nil
}
