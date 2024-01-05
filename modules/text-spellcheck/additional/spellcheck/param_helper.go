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

package spellcheck

import (
	"encoding/json"
)

type paramHelper struct{}

func newParamHelper() *paramHelper {
	return &paramHelper{}
}

func (p *paramHelper) getTexts(argumentModuleParams map[string]interface{}) (string, []string, error) {
	if argumentModuleParams["nearText"] != nil {
		texts, err := p.parseNearText(argumentModuleParams["nearText"])
		return "nearText", texts, err
	}
	if argumentModuleParams["ask"] != nil {
		texts, err := p.parseAsk(argumentModuleParams["ask"])
		return "ask", texts, err
	}
	return "", []string{}, nil
}

func (p *paramHelper) toJsonParam(arg interface{}) (map[string]interface{}, error) {
	data, err := json.Marshal(arg)
	if err != nil {
		return nil, err
	}
	var argument map[string]interface{}
	err = json.Unmarshal(data, &argument)
	if err != nil {
		return nil, err
	}
	return argument, nil
}

func (p *paramHelper) parseNearText(arg interface{}) ([]string, error) {
	argument, err := p.toJsonParam(arg)
	if err != nil {
		return nil, err
	}
	if argument["Values"] != nil {
		values, ok := argument["Values"].([]interface{})
		if ok {
			texts := []string{}
			for _, value := range values {
				strVal, ok := value.(string)
				if ok {
					texts = append(texts, strVal)
				}
			}
			return texts, nil
		}
	}
	return []string{}, nil
}

func (p *paramHelper) parseAsk(arg interface{}) ([]string, error) {
	argument, err := p.toJsonParam(arg)
	if err != nil {
		return nil, err
	}
	if argument["Question"] != nil {
		question, ok := argument["Question"].(string)
		if ok {
			return []string{question}, nil
		}
	}
	return []string{}, nil
}
