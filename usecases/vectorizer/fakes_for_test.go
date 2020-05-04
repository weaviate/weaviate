//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package vectorizer

import "context"

type fakeClient struct {
	lastInput []string
}

func (c *fakeClient) VectorForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, error) {
	c.lastInput = corpi
	return []float32{0, 1, 2, 3}, nil
}

func (c *fakeClient) VectorForWord(ctx context.Context, word string) ([]float32, error) {
	c.lastInput = []string{word}
	return []float32{3, 2, 1, 0}, nil

}
func (c *fakeClient) NearestWordsByVector(ctx context.Context,
	vector []float32, n int, k int) ([]string, []float32, error) {
	return []string{"word1", "word2"}, []float32{0.1, 0.2}, nil
}

func (c *fakeClient) IsWordPresent(ctx context.Context, word string) (bool, error) {
	return true, nil
}
