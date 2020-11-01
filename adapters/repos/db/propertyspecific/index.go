package propertyspecific

import (
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/geo"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// Index - for now - only supports a Geo index as a property-specific index.
// This could be extended in the future, for example to allow vectorization of
// single properties, as opposed to only allowing vectorization of the entire
// object.
type Index struct {
	Name     string
	Type     schema.DataType
	GeoIndex *geo.Index
}

// Indices is a collection of property-specific Indices by propname
type Indices map[string]Index

// ByProp retrieves a property-specific index by prop name. Second argument is
// false, if the index doesn't exist.
func (i Indices) ByProp(propName string) (Index, bool) {
	index, ok := i[propName]
	return index, ok
}
