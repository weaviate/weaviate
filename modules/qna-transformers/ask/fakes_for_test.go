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

package ask

type fakeTransformer struct{}

func (t *fakeTransformer) Transform(in []string) ([]string, error) {
	if len(in) == 1 && in[0] == "transform this" {
		return []string{"transformed text"}, nil
	}
	return in, nil
}
