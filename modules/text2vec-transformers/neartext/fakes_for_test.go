//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package neartext

type fakeTransformer struct{}

func (t *fakeTransformer) Transform(in []string) ([]string, error) {
	result := make([]string, len(in))
	for i, txt := range in {
		if txt == "transform this" {
			result[i] = "transformed text"
		} else {
			result[i] = txt
		}
	}
	return result, nil
}
