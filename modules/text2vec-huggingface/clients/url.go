//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package clients

import "fmt"

type huggingFaceUrlBuilder struct {
	origin   string
	pathMask string
}

func newHuggingFaceUrlBuilder() *huggingFaceUrlBuilder {
	return &huggingFaceUrlBuilder{
		origin:   "https://api-inference.huggingface.co",
		pathMask: "/pipeline/feature-extraction/%s",
	}
}

func (o *huggingFaceUrlBuilder) url(model string) string {
	return fmt.Sprintf("%s%s", o.origin, o.getPath(model))
}

func (o *huggingFaceUrlBuilder) getPath(model string) string {
	return fmt.Sprintf(o.pathMask, model)
}
