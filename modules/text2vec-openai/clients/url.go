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

type openAIUrl struct {
	docType, model, vectorizationType string
}

func newDocumentVectorizerUrl(docType, model string) *openAIUrl {
	return &openAIUrl{docType, model, "doc"}
}

func newQueryVectorizerUrl(docType, model string) *openAIUrl {
	return &openAIUrl{docType, model, "query"}
}

func (o *openAIUrl) url() string {
	return fmt.Sprintf("https://api.openai.com/v1/engines/%s-search-%s-%s-001/embeddings", o.docType, o.model, o.vectorizationType)
}
