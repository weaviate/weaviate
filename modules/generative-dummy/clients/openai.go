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
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
)

var compile, _ = regexp.Compile(`{([\w\s]*?)}`)

type dummy struct {
	logger logrus.FieldLogger
}

func New(logger logrus.FieldLogger) *dummy {
	return &dummy{
		logger: logger,
	}
}

func (v *dummy) GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forPrompt, err := v.generateForPrompt(textProperties, prompt)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forPrompt, options, debug)
}

func (v *dummy) GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, options interface{}, debug bool, cfg moduletools.ClassConfig) (*modulecapabilities.GenerateResponse, error) {
	forTask, err := v.generatePromptForTask(textProperties, task)
	if err != nil {
		return nil, err
	}
	return v.Generate(ctx, cfg, forTask, options, debug)
}

func (v *dummy) Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string, options interface{}, debug bool) (*modulecapabilities.GenerateResponse, error) {
	cls := cfg.ClassByModuleName("generative-dummy")
	settings := ""
	for key, val := range cls {
		settings += fmt.Sprintf("%s=%s,", key, val)
	}
	result := "You want me to generate something based on the following prompt: " + prompt + ". With settings: " + settings + ". I'm sorry, I'm just a dummy and can't generate anything."
	return &modulecapabilities.GenerateResponse{
		Result: &result,
	}, nil
}

func (v *dummy) generatePromptForTask(textProperties []map[string]string, task string) (string, error) {
	marshal, err := json.Marshal(textProperties)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`'%v:
%v`, task, string(marshal)), nil
}

func (v *dummy) generateForPrompt(textProperties map[string]string, prompt string) (string, error) {
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
