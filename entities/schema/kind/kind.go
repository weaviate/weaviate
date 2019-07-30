//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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

// Thing Kind represents real-life things, such as objects and persons
const Thing Kind = "thing"

// Action Kind represents events and processes
const Action Kind = "action"

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
	case "thing":
		return Thing, nil
	case "action":
		return Action, nil
	default:
		return "", fmt.Errorf("invalid kind: %s", name)
	}
}
