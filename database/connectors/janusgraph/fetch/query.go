package fetch

import (
	"fmt"
	"strings"

	"github.com/creativesoftwarefdn/weaviate/contextionary"
	"github.com/creativesoftwarefdn/weaviate/database/connectors/janusgraph/state"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/fetch"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/davecgh/go-spew/spew"
)

// Query prepares a Local->Fetch Query. Can be built with String(). Create with
// NewQuery() to be sure that all required properties are set
type Query struct {
	params     fetch.Params
	nameSource nameSource
	typeSource typeSource
}

// NewQuery is the preferred way to create a query
func NewQuery(p fetch.Params, ns nameSource, ts typeSource) *Query {
	return &Query{
		params:     p,
		nameSource: ns,
		typeSource: ts,
	}
}

// nameSource can only be used after verifying that the property exists, if in
// doubt use typeSource first
type nameSource interface {
	GetMappedPropertyName(className schema.ClassName, propName schema.PropertyName) state.MappedPropertyName
	GetMappedClassName(className schema.ClassName) state.MappedClassName
}

type typeSource interface {
	GetProperty(kind kind.Kind, className schema.ClassName,
		propName schema.PropertyName) (error, *models.SemanticSchemaClassProperty)
	FindPropertyDataType(dataType []string) (schema.PropertyDataType, error)
}

// String builds the query and returns it as a string
func (b *Query) String() (string, error) {
	properties, err := b.properties()
	if err != nil {
		return "", err
	}

	q := gremlin.New().
		Raw("g.V()").
		HasString("kind", b.params.Kind.Name()).
		And(properties...).
		Raw(`.valueMap("uuid", "classId")`)

	return q.String(), nil
}

func (b *Query) properties() ([]*gremlin.Query, error) {
	var queries []*gremlin.Query

	for _, prop := range b.params.Properties {
		q, err := b.property(prop)
		if err != nil {
			return nil, fmt.Errorf("could not build property '%v': %s", prop, err)
		}

		queries = append(queries, q)
	}

	return queries, nil
}

func (b *Query) property(prop fetch.Property) (*gremlin.Query, error) {
	var filterAlternatives []*gremlin.Query

	for _, className := range b.params.PossibleClassNames.Results {
		alternative := b.combineClassWithPropNames(className.Name, prop.PossibleNames.Results, prop.Match)
		if alternative == nil {
			continue
		}

		filterAlternatives = append(filterAlternatives, alternative)
	}

	q := gremlin.New().
		Or(filterAlternatives...)

	return q, nil
}

func (b *Query) combineClassWithPropNames(className string, propNames []contextionary.SearchResult,
	match fetch.PropertyMatch) *gremlin.Query {
	var combinations []string

	for _, propName := range propNames {
		combination := b.combineClassWithPropName(className, propName.Name, match)
		if combination == nil {
			continue
		}

		combinations = append(combinations, combination.String())
	}

	return gremlin.New().Raw(strings.Join(combinations, ", "))
}

func (b *Query) combineClassWithPropName(className string, propName string,
	match fetch.PropertyMatch) *gremlin.Query {
	class := schema.ClassName(className)
	prop := schema.PropertyName(propName)

	err, _ := b.typeSource.GetProperty(b.params.Kind, class, prop)
	if err != nil {
		// this class property combination does not exist, simply skip it
		spew.Dump(err)
		return nil
	}

	mappedClass := b.nameSource.GetMappedClassName(class)
	mappedProp := b.nameSource.GetMappedPropertyName(class, prop)

	return gremlin.New().
		HasString("classId", string(mappedClass)).
		Has(string(mappedProp), gremlin.EqString(match.Value.Value.(string)))
}
