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
	"bytes"
	"regexp"

	"github.com/pkg/errors"
)

type likeRegexp struct {
	optimizable bool
	min         []byte
	regexp      *regexp.Regexp
}

func parseLikeRegexp(in []byte) (*likeRegexp, error) {
	r, err := regexp.Compile(transformLikeStringToRegexp(in))
	if err != nil {
		return nil, errors.Wrap(err, "compile regex from 'like' string")
	}

	min, ok := optimizable(in)
	return &likeRegexp{
		regexp:      r,
		min:         min,
		optimizable: ok,
	}, nil
}

func transformLikeStringToRegexp(in []byte) string {
	in = bytes.ReplaceAll(in, []byte("?"), []byte("."))
	in = bytes.ReplaceAll(in, []byte("*"), []byte(".*"))
	return "^" + string(in) + "$"
}

func optimizable(in []byte) ([]byte, bool) {
	maxCharsWithoutWildcard := 0
	for _, char := range in {
		if isWildcardCharacter(char) {
			break
		}
		maxCharsWithoutWildcard++
	}

	return in[:maxCharsWithoutWildcard], maxCharsWithoutWildcard > 0
}

func isWildcardCharacter(in byte) bool {
	return in == '?' || in == '*'
}
