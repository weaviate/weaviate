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

type voyageaiUrlBuilder struct {
	origin   string
	pathMask string
}

func newVoyageAIUrlBuilder() *voyageaiUrlBuilder {
	return &voyageaiUrlBuilder{
		origin:   "https://api.voyageai.com/v1",
		pathMask: "/multimodalembeddings",
	}
}

func (b *voyageaiUrlBuilder) url(baseURL string) string {
	if baseURL != "" {
		return baseURL + b.pathMask
	}
	return b.origin + b.pathMask
}
