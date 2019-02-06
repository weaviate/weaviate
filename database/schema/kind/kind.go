/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package kind

import (
	"fmt"
	"strings"
)

type Kind string

const THING_KIND Kind = "thing"
const ACTION_KIND Kind = "action"
const NETWORK_THING_KIND Kind = "network_thing"
const NETWORK_ACTION_KIND Kind = "network_action"

func (k *Kind) Name() string {
	return string(*k)
}

func (k *Kind) TitleizedName() string {
	return strings.Title(k.Name())
}

func (k *Kind) AllCapsName() string {
	return strings.ToUpper(k.Name())
}

func KindByName(name string) Kind {
	switch name {
	case "thing":
		return THING_KIND
	case "action":
		return ACTION_KIND
	default:
		panic(fmt.Sprintf("No such kind %s", name))
	}
}
