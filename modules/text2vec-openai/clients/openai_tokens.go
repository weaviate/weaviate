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

import (
	"strings"

	"github.com/weaviate/tiktoken-go"
)

func GetTokensCount(model string, input string, tke *tiktoken.Tiktoken) int {
	tokensPerMessage := 3
	if strings.HasPrefix(model, "gpt-3.5-turbo") {
		tokensPerMessage = 4
	}

	tokensCount := tokensPerMessage
	tokensCount += len(tke.Encode(input, nil, nil))
	return tokensCount
}
