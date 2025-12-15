//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"fmt"
	"strings"

	"github.com/weaviate/tiktoken-go"
)

func getTokensCount(model string, messages []message) (int, error) {
	tke, err := tiktoken.EncodingForModel(model)
	if err != nil {
		return 0, fmt.Errorf("encoding for model %s: %w", model, err)
	}

	tokensPerMessage := 3
	if strings.HasPrefix(model, "gpt-3.5-turbo") {
		tokensPerMessage = 4
	}

	tokensPerName := 1
	if strings.HasPrefix(model, "gpt-3.5-turbo") {
		tokensPerName = -1
	}

	tokensCount := 3
	for _, message := range messages {
		tokensCount += tokensPerMessage
		tokensCount += len(tke.Encode(message.Role, nil, nil))
		tokensCount += len(tke.Encode(parseMessageContent(message.Content), nil, nil))
		if message.Name != "" {
			tokensCount += tokensPerName
			tokensCount += len(tke.Encode(message.Name, nil, nil))
		}
	}
	return tokensCount, nil
}

func parseMessageContent(content interface{}) string {
	switch c := content.(type) {
	case string:
		return c
	case []interface{}:
		var contents []string
		for i := range c {
			switch input := c[i].(type) {
			case contentText:
				contents = append(contents, input.Text)
			case contentImage:
				if input.ImageURL.URL == nil {
					continue
				}
				contents = append(contents, *input.ImageURL.URL)
			default:
				// do nothing
			}
		}
		return strings.Join(contents, " ")
	default:
		return ""
	}
}
