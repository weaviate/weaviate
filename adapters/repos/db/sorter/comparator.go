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

package sorter

import (
	"strings"
	"time"
)

type comparator struct {
	order string
}

func (s comparator) compareString(a, b *string) bool {
	if a != nil && b != nil {
		if s.order == "desc" {
			return strings.ToLower(*a) > strings.ToLower(*b)
		}
		return strings.ToLower(*a) < strings.ToLower(*b)
	}
	return s.handleNil(a == nil, b == nil)
}

func (s comparator) compareFloat64(a, b *float64) bool {
	if a != nil && b != nil {
		if s.order == "desc" {
			return *a > *b
		}
		return *a < *b
	}
	return s.handleNil(a == nil, b == nil)
}

func (s comparator) compareDate(a, b *time.Time) bool {
	if a != nil && b != nil {
		if s.order == "desc" {
			return a.After(*b)
		}
		return a.Before(*b)
	}
	return s.handleNil(a == nil, b == nil)
}

func (s comparator) compareBool(a, b *bool) bool {
	if a != nil && b != nil {
		if s.order == "desc" {
			return !*a || *b
		}
		return !(!*a || *b)
	}
	return s.handleNil(a == nil, b == nil)
}

func (s comparator) handleNil(a, b bool) bool {
	if a && b {
		return false
	}
	if a {
		// if s.order == "desc" return false if not then true
		return s.order != "desc"
	}
	// if s.order == "desc" return true if not then false
	return s.order == "desc"
}
