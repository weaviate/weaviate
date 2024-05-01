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

type wcsEmbedUrlBuilder struct {
	origin   string
	pathMask string
}

func newWCSEmbedUrlBuilder() *wcsEmbedUrlBuilder {
	return &wcsEmbedUrlBuilder{
		origin:   "https://some.path.to.wcs.embed",
		pathMask: "/v1/embed",
	}
}

func (c *wcsEmbedUrlBuilder) url(baseURL string) string {
	if baseURL != "" {
		return fmt.Sprintf("%s%s", baseURL, c.pathMask)
	}
	return fmt.Sprintf("%s%s", c.origin, c.pathMask)
}
