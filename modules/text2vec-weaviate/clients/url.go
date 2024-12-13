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

import "fmt"

type weaviateEmbedUrlBuilder struct {
	origin   string
	pathMask string
}

func newWeaviateEmbedUrlBuilder() *weaviateEmbedUrlBuilder {
	return &weaviateEmbedUrlBuilder{
		origin:   "https://api.embedding.weaviate.io",
		pathMask: "/v1/embeddings/embed",
	}
}

func (c *weaviateEmbedUrlBuilder) url(baseURL string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.pathMask)
	}
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}
