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
	"runtime"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

var _NUMCPU = runtime.NumCPU()

func ValidateConfig(conf *models.InvertedIndexConfig) error {
	if conf.CleanupIntervalSeconds < 0 {
		return errors.Errorf("cleanup interval seconds must be > 0")
	}

	err := validateBM25Config(conf.Bm25)
	if err != nil {
		return err
	}

	err = validateStopwordConfig(conf.Stopwords)
	if err != nil {
		return err
	}

	return nil
}

func ConfigFromModel(iicm *models.InvertedIndexConfig) schema.InvertedIndexConfig {
	var conf schema.InvertedIndexConfig

	conf.IndexTimestamps = iicm.IndexTimestamps
	conf.IndexNullState = iicm.IndexNullState
	conf.IndexPropertyLength = iicm.IndexPropertyLength

	if iicm.Bm25 == nil {
		conf.BM25.K1 = float64(config.DefaultBM25k1)
		conf.BM25.B = float64(config.DefaultBM25b)
	} else {
		conf.BM25.K1 = float64(iicm.Bm25.K1)
		conf.BM25.B = float64(iicm.Bm25.B)
	}

	if iicm.Stopwords == nil {
		conf.Stopwords = models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}
	} else {
		conf.Stopwords.Preset = iicm.Stopwords.Preset
		conf.Stopwords.Additions = iicm.Stopwords.Additions
		conf.Stopwords.Removals = iicm.Stopwords.Removals
	}

	return conf
}

func validateBM25Config(conf *models.BM25Config) error {
	if conf == nil {
		return nil
	}

	if conf.K1 < 0 {
		return errors.Errorf("BM25.k1 must be >= 0")
	}
	if conf.B < 0 || conf.B > 1 {
		return errors.Errorf("BM25.b must be <= 0 and <= 1")
	}

	return nil
}

func validateStopwordConfig(conf *models.StopwordConfig) error {
	if conf == nil {
		conf = &models.StopwordConfig{}
	}

	if conf.Preset == "" {
		conf.Preset = stopwords.EnglishPreset
	}

	if _, ok := stopwords.Presets[conf.Preset]; !ok {
		return errors.Errorf("stopwordPreset '%s' does not exist", conf.Preset)
	}

	err := validateStopwordAdditionsRemovals(conf)
	if err != nil {
		return err
	}

	return nil
}

func validateStopwordAdditionsRemovals(conf *models.StopwordConfig) error {
	// the same stopword cannot exist
	// in both additions and removals
	foundAdditions := make(map[string]int)

	for idx, add := range conf.Additions {
		if strings.TrimSpace(add) == "" {
			return errors.Errorf("cannot use whitespace in stopword.additions")
		}

		// save the index of the addition since it
		// is readily available here. we will need
		// this below when trimming additions that
		// already exist in the selected preset
		foundAdditions[add] = idx
	}

	for _, rem := range conf.Removals {
		if strings.TrimSpace(rem) == "" {
			return errors.Errorf("cannot use whitespace in stopword.removals")
		}

		if _, ok := foundAdditions[rem]; ok {
			return errors.Errorf(
				"found '%s' in both stopwords.additions and stopwords.removals", rem)
		}
	}

	removeStopwordAdditionsIfInPreset(conf, foundAdditions)
	return nil
}

func removeStopwordAdditionsIfInPreset(conf *models.StopwordConfig, foundAdditions map[string]int) {
	presets := stopwords.Presets[conf.Preset]

	// if any of the elements in stopwords.additions
	// already exist in the preset, mark it as to
	// be removed
	indicesToRemove := make(map[int]bool)
	for _, preset := range presets {
		if idx, ok := foundAdditions[preset]; ok {
			indicesToRemove[idx] = true
		}
	}

	if len(indicesToRemove) == 0 {
		return
	}

	// take remaining additions, build new list
	var trimmedAdditions []string
	for idx, add := range conf.Additions {
		if _, ok := indicesToRemove[idx]; !ok {
			trimmedAdditions = append(trimmedAdditions, add)
		}
	}
	conf.Additions = trimmedAdditions
}
