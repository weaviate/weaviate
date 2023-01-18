//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import "fmt"

type cohereUrlBuilder struct {
	origin   string
	pathMask string
}

func newCohereUrlBuilder() *cohereUrlBuilder {
	return &cohereUrlBuilder{
		origin:   "https://api.cohere.ai",
		pathMask: "/embed",
	}
}

func (c *cohereUrlBuilder) url() string {
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}
