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

package generative

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

func Texts(properties []*modulecapabilities.GenerateProperties) []map[string]string {
	texts := make([]map[string]string, 0, len(properties))
	for _, prop := range properties {
		if prop != nil && len(prop.Text) > 0 {
			texts = append(texts, prop.Text)
		}
	}
	return texts
}

func Blobs(properties []*modulecapabilities.GenerateProperties) []map[string]string {
	blobs := make([]map[string]string, len(properties))
	for _, prop := range properties {
		if prop != nil && len(prop.Blob) > 0 {
			blobs = append(blobs, prop.Blob)
		}
	}
	return blobs
}

func ParseImageProperties(inputImages []string, storedImageProperties []map[string]string) []string {
	if len(inputImages) > 0 && len(storedImageProperties) > 0 {
		images := make([]string, len(inputImages))
		for _, imageProperties := range storedImageProperties {
			if len(imageProperties) == 0 {
				continue
			}
			for i, imageProperty := range inputImages {
				if image, ok := imageProperties[imageProperty]; ok {
					images[i] = image // in this case, the image has been retrieved from the DB
				} else {
					images[i] = imageProperty // in this case, the image is a base64 supplied in inputImages
				}
			}
		}
		return images
	}
	return nil
}

func MakeTaskPrompt(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", errors.Wrap(err, "marshal text properties")
	}
	task = compile.ReplaceAllStringFunc(task, func(match string) string {
		match = strings.Trim(match, "{}")
		for _, textProperty := range textProperties {
			if val, ok := textProperty[match]; ok {
				return val
			}
		}
		return match
	})
	return fmt.Sprintf(task, marshal), nil
}

func MakeSinglePrompt(textProperties map[string]string, prompt string) (string, error) {
	all := compile.FindAll([]byte(prompt), -1)
	for _, match := range all {
		originalProperty := string(match)
		replacedProperty := compile.FindStringSubmatch(originalProperty)[1]
		replacedProperty = strings.TrimSpace(replacedProperty)
		value := textProperties[replacedProperty]
		if value == "" {
			return "", errors.Errorf("Following property has empty value: '%v'. Make sure you spell the property name correctly, verify that the property exists and has a value", replacedProperty)
		}
		prompt = strings.ReplaceAll(prompt, originalProperty, value)
	}
	return prompt, nil
}
