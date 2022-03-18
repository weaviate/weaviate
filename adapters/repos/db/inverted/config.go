//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package inverted

import (
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/config"
)

func ValidateConfig(conf *models.InvertedIndexConfig) error {
	if conf.CleanupIntervalSeconds < 0 {
		return errors.Errorf("cleanup interval seconds must be > 0")
	}

	err := validateBM25Config(conf.Bm25)
	if err != nil {
		return err
	}

	return nil
}

func ConfigFromModel(iicm *models.InvertedIndexConfig) schema.InvertedIndexConfig {
	var conf schema.InvertedIndexConfig

	conf.CleanupIntervalSeconds = iicm.CleanupIntervalSeconds

	if iicm.Bm25 == nil {
		conf.BM25.K1 = float64(config.DefaultBM25k1)
		conf.BM25.B = float64(config.DefaultBM25b)
	} else {
		conf.BM25.K1 = float64(iicm.Bm25.K1)
		conf.BM25.B = float64(iicm.Bm25.B)
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
