package kind

import (
	"fmt"
)

type Kind string

const THING_KIND Kind = "thing"
const ACTION_KIND Kind = "action"

func (k *Kind) Name() string {
	return string(*k)
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
