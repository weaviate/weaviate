//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helpers

import (
	"strings"
	"unicode"
)

func TokenizeString(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return unicode.IsSpace(c)
	})
	return parts
}

func TokenizeText(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	})
	return parts
}

func TokenizeTextKeepWildcards(in string) []string {
	parts := strings.FieldsFunc(in, func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c) && c != '?' && c != '*'
	})
	return parts
}
