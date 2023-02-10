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

type huggingFaceUrlBuilder struct {
	origin   string
	pathMask string
	customOrigin string
	customPathMask string
}

func newHuggingFaceUrlBuilder(customOrigin string, customPathMask string) *huggingFaceUrlBuilder {
	return &huggingFaceUrlBuilder{
		origin:   "https://api-inference.huggingface.co",
		pathMask: "/pipeline/feature-extraction/%s",
		customOrigin: customOrigin,
		customPathMask: customPathMask,
	}
}

func (o *huggingFaceUrlBuilder) url(model string) string {
	origin := o.origin
	pathMask := o.getPath(model)
	if o.customOrigin != "" {
		origin = o.customOrigin
	}
	if o.customPathMask != "" {
		pathMask = o.customPathMask
	}
	return fmt.Sprintf("%s%s", origin, pathMask)
}

func (o *huggingFaceUrlBuilder) getPath(model string) string {
	return fmt.Sprintf(o.pathMask, model)
}
