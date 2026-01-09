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

package cohere

import "fmt"

type UrlBuilder interface {
	URL(baseURL string) string
}

type cohereUrlBuilder struct {
	origin string
	path   string
}

func NewCohereUrlBuilder(path string) UrlBuilder {
	return &cohereUrlBuilder{
		origin: "https://api.cohere.ai",
		path:   path,
	}
}

func (c *cohereUrlBuilder) URL(baseURL string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.path)
	}
	return fmt.Sprintf("%s%s", c.origin, c.path)
}
