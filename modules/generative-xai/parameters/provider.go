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

package parameters

import "github.com/weaviate/weaviate/entities/modulecapabilities"

const Name = "xai"

func AdditionalGenerativeParameters(client modulecapabilities.GenerativeClient) map[string]modulecapabilities.GenerativeProperty {
	return map[string]modulecapabilities.GenerativeProperty{
		Name: {Client: client, RequestParamsFunction: input, ResponseParamsFunction: output, ExtractRequestParamsFunction: extract},
	}
}
