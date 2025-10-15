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

package tokenizer

import (
	"fmt"
	"io"
	"strings"

	"github.com/ikawaha/kagome-dict/dict"
	"github.com/weaviate/weaviate/entities/models"
)

func createUserDictReaderFromModel(config *models.TokenizerUserDictConfig) io.Reader {
	if config == nil || config.Replacements == nil || len(config.Replacements) == 0 {
		return nil
	}

	var sb strings.Builder
	for _, r := range config.Replacements {
		// Each line in the user dictionary must have 4 columns:
		// surface form, reading, pronunciation, part of speech (optional)
		// We set reading and pronunciation to be the same as the surface form
		line := fmt.Sprintf("%s,%s,%s,\n", *r.Source, *r.Target, *r.Target)
		sb.WriteString(line)
	}

	return strings.NewReader(sb.String())
}

func NewUserDictFromModel(config *models.TokenizerUserDictConfig) (*dict.UserDict, error) {
	if config == nil || config.Replacements == nil || len(config.Replacements) == 0 {
		return nil, nil
	}

	r, err := dict.NewUserDicRecords(createUserDictReaderFromModel(config))
	if err != nil {
		return nil, err
	}
	return r.NewUserDict()
}

func InitUserDictTokenizers(config []*models.TokenizerUserDictConfig) (*KagomeTokenizers, error) {
	customTokenizers := &KagomeTokenizers{}
	if config == nil {
		return nil, nil
	}

	for _, tokenizerConfig := range config {
		if tokenizerConfig == nil || tokenizerConfig.Tokenizer == "" {
			continue
		}
		switch tokenizerConfig.Tokenizer {
		case models.PropertyTokenizationKagomeJa:
			tokenizerJa, err := initializeKagomeTokenizerJa(tokenizerConfig)
			if err != nil {
				return nil, err
			}
			customTokenizers.Japanese = tokenizerJa
		case models.PropertyTokenizationKagomeKr:
			tokenizerKr, err := initializeKagomeTokenizerKr(tokenizerConfig)
			if err != nil {
				return nil, err
			}
			customTokenizers.Korean = tokenizerKr
		default:
			return nil, fmt.Errorf("tokenizer %s does not support user dictionaries", tokenizerConfig.Tokenizer)
		}
	}
	return customTokenizers, nil
}
