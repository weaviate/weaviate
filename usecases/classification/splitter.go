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

package classification

// TODO: This code is duplicated across weaviate and contextionary which makes
// changes risky. Can we find a single source of truth for this logic

import (
	"strings"
	"unicode"
)

func newSplitter() *splitter {
	return &splitter{}
}

type splitter struct{}

func (s *splitter) Split(corpus string) []string {
	return strings.FieldsFunc(corpus, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})
}
