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

package client

func (c *client) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "Reranker - DigitalOcean Serverless Inference",
		"documentationHref": "https://docs.digitalocean.com/products/vector-databases/weaviate/how-to/ingest-and-retrieve/#rerank-candidates",
	}, nil
}
