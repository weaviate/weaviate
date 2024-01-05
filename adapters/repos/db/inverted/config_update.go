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

package inverted

import (
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
)

func ValidateUserConfigUpdate(initial, updated *models.InvertedIndexConfig) error {
	if updated.CleanupIntervalSeconds < 0 {
		return errors.Errorf("cleanup interval seconds must be > 0")
	}

	err := validateBM25ConfigUpdate(initial, updated)
	if err != nil {
		return err
	}

	err = validateInvertedIndexConfigUpdate(initial, updated)
	if err != nil {
		return err
	}

	err = validateStopwordsConfigUpdate(initial, updated)
	if err != nil {
		return err
	}

	return nil
}

func validateBM25ConfigUpdate(initial, updated *models.InvertedIndexConfig) error {
	if updated.Bm25 == nil {
		updated.Bm25 = &models.BM25Config{
			K1: initial.Bm25.K1,
			B:  initial.Bm25.B,
		}
		return nil
	}

	err := validateBM25Config(updated.Bm25)
	if err != nil {
		return err
	}

	return nil
}

func validateInvertedIndexConfigUpdate(initial, updated *models.InvertedIndexConfig) error {
	if updated.IndexPropertyLength != initial.IndexPropertyLength {
		return errors.New("IndexPropertyLength cannot be changed when updating a schema")
	}

	if updated.IndexNullState != initial.IndexNullState {
		return errors.New("IndexNullState cannot be changed when updating a schema")
	}

	return nil
}

func validateStopwordsConfigUpdate(initial, updated *models.InvertedIndexConfig) error {
	if updated.Stopwords == nil {
		updated.Stopwords = &models.StopwordConfig{
			Preset:    initial.Stopwords.Preset,
			Additions: initial.Stopwords.Additions,
			Removals:  initial.Stopwords.Removals,
		}
		return nil
	}

	err := validateStopwordConfig(updated.Stopwords)
	if err != nil {
		return err
	}

	return nil
}
