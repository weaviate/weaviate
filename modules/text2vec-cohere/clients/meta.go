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

func (v *vectorizer) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Cohere Module",
		"documentationHref": "https://docs.cohere.ai/embedding-wiki/",
	}, nil
}
