//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import "errors"

const (
	dangerZoneImagesHeader = "TypeSafe-Danger-Zone-Images"
	MaxBatchImages         = 4
	MaxBatchImageChars     = 24 << 20
)

type imageAsset struct {
	Type       string `json:"type"`
	DataBase64 string `json:"data_base64"`
	Format     string `json:"format"`
}

var errImagesNotJudged = errors.New("reranker-typesafeai does not judge images yet")

func checkImages(documents []string, positions []int) error {
	return errImagesNotJudged
}

func assetsOf(documents []string) (map[string]imageAsset, []string, error) {
	return nil, nil, errImagesNotJudged
}
