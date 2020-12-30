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

package kind

import (
	"fmt"
	"strings"
)

// Kind distinguishes between Thing and Action Kind. Other Kinds might be added
// in the future
type Kind string

// Object Kind represents real-life things, such as objects and persons and events and processes
const Object Kind = "object"

// Name returns the lowercaps name, such as thing, action
func (k *Kind) Name() string {
	return string(*k)
}

// TitleizedName uppercases the name, such as Thing, Action
func (k *Kind) TitleizedName() string {
	return strings.Title(k.Name())
}

// AllCapsName such as THING, ACTION
func (k *Kind) AllCapsName() string {
	return strings.ToUpper(k.Name())
}

// Parse parses a string into a typed Kind
func Parse(name string) (Kind, error) {
	switch name {
	case "object":
		return Object, nil
	default:
		return "", fmt.Errorf("invalid kind: %s", name)
	}
}
