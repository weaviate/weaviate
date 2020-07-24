package storobj

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
)

func (ko *Object) enrichSchemaTypes(schema map[string]interface{}) error {
	if schema == nil {
		return nil
	}

	for propName, value := range schema {
		var err error
		switch typed := value.(type) {
		case []interface{}:
			err = ko.enrichCrossRef(schema, propName, typed)
		default:
			continue
		}

		if err != nil {
			return errors.Wrapf(err, "property %q", propName)
		}
	}

	return nil
}

func (ko *Object) enrichCrossRef(schema map[string]interface{}, propName string,
	value []interface{}) error {

	parsed := make(models.MultipleRef, len(value))
	for i, elem := range value {
		asMap, ok := elem.(map[string]interface{})
		if !ok {
			return fmt.Errorf("crossref: expected element %d to be map - got %T", i, elem)
		}

		beacon, ok := asMap["beacon"]
		if !ok {
			return fmt.Errorf("crossref: expected element %d to have key %q - got %v", i, "beacon", elem)
		}

		beaconStr, ok := beacon.(string)
		if !ok {
			return fmt.Errorf("crossref: expected element %d.beacon to be string - got %T", i, beacon)
		}

		parsed[i] = &models.SingleRef{
			Beacon: strfmt.URI(beaconStr),
			// TODO: gh-1150 support underscore RefMeta
		}
	}

	schema[propName] = parsed

	return nil
}
