package kinds

import (
	"fmt"
	"regexp"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/common"
	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

func (t *Traverser) LocalGetClass(params LocalGetParams) (interface{}, error) {
	unlock, err := t.locks.LockConnector()
	if err != nil {
		return nil, fmt.Errorf("could not acquire lock: %v", err)
	}
	defer unlock()

	return t.repo.LocalGetClass(&params)
}

type LocalGetParams struct {
	Kind       kind.Kind
	Filters    *common_filters.LocalFilter
	ClassName  string
	Pagination *common.Pagination
	Properties []SelectProperty
}

type SelectProperty struct {
	Name string

	IsPrimitive bool

	// Include the __typename in all the Refs below.
	IncludeTypeName bool

	// Not a primitive type? Then select these properties.
	Refs []SelectClass
}

type SelectClass struct {
	ClassName     string
	RefProperties []SelectProperty
}

// FindSelectClass by specifying the exact class name
func (sp SelectProperty) FindSelectClass(className schema.ClassName) *SelectClass {
	for _, selectClass := range sp.Refs {
		if selectClass.ClassName == string(className) {
			return &selectClass
		}
	}

	return nil
}

// HasPeer returns true if any of the referenced classes are from the specified
// peer
func (sp SelectProperty) HasPeer(peerName string) bool {
	r := regexp.MustCompile(fmt.Sprintf("^%s__", peerName))
	for _, selectClass := range sp.Refs {
		if r.MatchString(selectClass.ClassName) {
			return true
		}
	}

	return false
}
