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

type openAIUrlBuilder struct {
	origin   string
	pathMask string
}

func newOpenAIUrlBuilder() *openAIUrlBuilder {
	return &openAIUrlBuilder{
		origin:   "https://api.openai.com",
		pathMask: "/v1/engines/%s-search-%s-%s-001/embeddings",
	}
}

func (o *openAIUrlBuilder) documentUrl(docType, model string) string {
	return fmt.Sprintf("%s%s", o.origin, o.getPath(docType, model, o.getVectorizationType(docType, "document")))
}

func (o *openAIUrlBuilder) queryUrl(docType, model string) string {
	return fmt.Sprintf("%s%s", o.origin, o.getPath(docType, model, o.getVectorizationType(docType, "query")))
}

func (o *openAIUrlBuilder) getPath(docType, model, vectorizationType string) string {
	return fmt.Sprintf(o.pathMask, docType, model, vectorizationType)
}

func (o *openAIUrlBuilder) getVectorizationType(docType, action string) string {
	if action == "document" {
		if docType == "code" {
			return "code"
		}
		return "doc"
	} else {
		if docType == "code" {
			return "text"
		}
		return "query"
	}
}
