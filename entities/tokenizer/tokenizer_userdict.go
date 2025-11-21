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

package tokenizer

import (
	"fmt"
	"io"
	"strings"

	"github.com/ikawaha/kagome-dict/dict"
	"github.com/weaviate/weaviate/entities/models"
)

func AddCustomDict(className string, configs []*models.TokenizerUserDictConfig) error {
	customTokenizers.Delete(className)
	tokenizers, err := initUserDictTokenizers(configs)
	if err != nil {
		return err
	}
	if tokenizers == nil {
		return nil
	}
	customTokenizers.Store(className, tokenizers)
	return nil
}

func createUserDictReaderFromModel(config *models.TokenizerUserDictConfig) (io.Reader, error) {
	if config == nil || config.Replacements == nil || len(config.Replacements) == 0 {
		return nil, nil
	}

	var sb strings.Builder
	for _, r := range config.Replacements {
		if r.Source == nil || r.Target == nil {
			return nil, fmt.Errorf("both source and target must be set")
		}
		// Each line in the user dictionary must have 4 columns:
		// surface form, reading, pronunciation, part of speech (optional)
		// We set reading and pronunciation to be the same as the surface form
		line := fmt.Sprintf("%s,%s,%s,\n", *r.Source, *r.Target, *r.Target)
		sb.WriteString(line)
	}

	return strings.NewReader(sb.String()), nil
}

func NewUserDictFromModel(config *models.TokenizerUserDictConfig) (*dict.UserDict, error) {
	if config == nil || config.Replacements == nil || len(config.Replacements) == 0 {
		return nil, nil
	}

	dictReader, err := createUserDictReaderFromModel(config)
	if err != nil {
		return nil, err
	}
	r, err := dict.NewUserDicRecords(dictReader)
	if err != nil {
		return nil, err
	}
	return r.NewUserDict()
}

func initUserDictTokenizers(config []*models.TokenizerUserDictConfig) (*KagomeTokenizers, error) {
	var tokenizers *KagomeTokenizers
	if len(config) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{})
	for _, tokenizerConfig := range config {
		if tokenizerConfig == nil {
			continue
		}
		if _, ok := seen[tokenizerConfig.Tokenizer]; ok {
			return nil, fmt.Errorf("found duplicate tokenizer '%s' in tokenizer user dict config", tokenizerConfig.Tokenizer)
		}
		if tokenizers == nil {
			tokenizers = &KagomeTokenizers{}
		}
		seen[tokenizerConfig.Tokenizer] = struct{}{}

		switch tokenizerConfig.Tokenizer {
		case models.PropertyTokenizationKagomeJa:
			tokenizerJa, err := initializeKagomeTokenizerJa(tokenizerConfig)
			if err != nil {
				return nil, err
			}
			tokenizers.Japanese = tokenizerJa
		case models.PropertyTokenizationKagomeKr:
			tokenizerKr, err := initializeKagomeTokenizerKr(tokenizerConfig)
			if err != nil {
				return nil, err
			}
			tokenizers.Korean = tokenizerKr
		default:
			return nil, fmt.Errorf("tokenizer %s does not support user dictionaries", tokenizerConfig.Tokenizer)
		}
	}
	return tokenizers, nil
}
