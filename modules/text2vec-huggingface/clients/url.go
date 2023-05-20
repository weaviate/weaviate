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

import (
	"fmt"

	"github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
)

const (
	DefaultOrigin = "https://api-inference.huggingface.co"
	DefaultPath   = "pipeline/feature-extraction"
)

func getHuggingFaceUrl(config ent.VectorizationConfig) string {
	url := config.EndpointURL
	if url == "" {
		url = fmtUrl(config)
	}

	return url
}

func fmtUrl(config ent.VectorizationConfig) string {
	return fmt.Sprintf("%s/%s/%s", DefaultOrigin, DefaultPath, config.Model)
}
