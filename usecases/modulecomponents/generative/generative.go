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

func Text(properties *modulecapabilities.GenerateProperties) map[string]string {
	if properties != nil && len(properties.Text) > 0 {
		return properties.Text
	}
	return nil
}

func Texts(properties []*modulecapabilities.GenerateProperties) []map[string]string {
	texts := make([]map[string]string, 0, len(properties))
	for _, prop := range properties {
		if prop != nil && len(prop.Text) > 0 {
			texts = append(texts, prop.Text)
		}
	}
	return texts
}

func Blobs(properties []*modulecapabilities.GenerateProperties) []map[string]*string {
	blobs := make([]map[string]*string, 0, len(properties))
	for _, prop := range properties {
		if prop != nil && len(prop.Blob) > 0 {
			blobs = append(blobs, prop.Blob)
		}
	}
	return blobs
}

// ParseImageProperties parses the user-supplied base64 images in inputImages and server-stored base64 images in storedImagePropertiesArray based on the inputImageProperties.
//
// It returns a slice of pointers to base64 strings in order to optimise for memory usage when dealing with large images.
func ParseImageProperties(inputBase64Images []*string, inputImagePropertyNames []string, storedBase64ImagesArray []map[string]*string) []*string {
	images := []*string{}
	if len(storedBase64ImagesArray) > 0 {
		for _, storedBase64Images := range storedBase64ImagesArray {
			for _, inputImagePropertyName := range inputImagePropertyNames {
				images = append(images, storedBase64Images[inputImagePropertyName])
			}
		}
	}
	images = append(images, inputBase64Images...)
	return images
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
	if len(marshal) > 0 {
		return fmt.Sprintf("%s: %s", task, marshal), nil
	}
	return fmt.Sprint(task), nil
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
