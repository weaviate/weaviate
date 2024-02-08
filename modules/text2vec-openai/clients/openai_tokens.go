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
	"fmt"
	"strings"

	"github.com/pkoukk/tiktoken-go"
)

func GetTokensCount(model string, input string) (int, error) {
	tke, err := tiktoken.EncodingForModel(model)
	if err != nil {
		return 0, fmt.Errorf("encoding for model %s: %w", model, err)
	}

	tokensPerMessage := 3
	if strings.HasPrefix(model, "gpt-3.5-turbo") {
		tokensPerMessage = 4
	}

	tokensCount := tokensPerMessage
	tokensCount += len(tke.Encode(input, nil, nil))
	return tokensCount, nil
}
