package network_get

import (
	"fmt"

	"github.com/graphql-go/graphql"
)

func ResolveThing(p graphql.ResolveParams) (interface{}, error) {
	firstLevel, ok := p.Source.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected source to be map[string]interface{}, was %#v", p.Source)
	}

	secondLevel, ok := firstLevel["Things"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected source to be map[string]map[string][]interface{}, was %#v", firstLevel["Things"])
	}

	fmt.Printf("about to return %v", secondLevel[p.Info.FieldName])
	return secondLevel[p.Info.FieldName], nil
}
