package grouper

import (
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/search"
)

type valueType int

const (
	numerical valueType = iota
	textual
	boolean
	reference
	geo
	unknown
)

type valueGroup struct {
	values    []interface{}
	valueType valueType
	name      string
}

func (g group) flattenMerge() (search.Result, error) {
	values := g.makeValueGroups()
	merged, err := mergeValueGroups(values)
	if err != nil {
		return search.Result{}, fmt.Errorf("merge values: %v", err)
	}

	return search.Result{
		Schema: merged,
	}, nil
}

func (g group) makeValueGroups() map[string]valueGroup {
	values := map[string]valueGroup{}

	for _, elem := range g.Elements {
		if elem.Schema == nil {
			continue
		}

		for propName, propValue := range elem.Schema.(map[string]interface{}) {
			current, ok := values[propName]
			if !ok {
				current = valueGroup{
					values:    []interface{}{propValue},
					valueType: valueTypeOf(propValue),
					name:      propName,
				}
				values[propName] = current
				continue
			}

			current.values = append(current.values, propValue)
			values[propName] = current
		}
	}

	return values
}

func mergeValueGroups(props map[string]valueGroup) (map[string]interface{}, error) {
	mergedProps := map[string]interface{}{}

	for propName, group := range props {
		var (
			res interface{}
			err error
		)
		switch group.valueType {
		case textual:
			res, err = mergeTextualProps(group.values)
		case numerical:
			res, err = mergeNumericalProps(group.values)
		case boolean:
			res, err = mergeBooleanProps(group.values)
		case geo:
			res, err = mergeGeoProps(group.values)
		default:
			err = fmt.Errorf("unrecognized value type")
		}
		if err != nil {
			return nil, fmt.Errorf("prop '%s': %v", propName, err)
		}

		mergedProps[propName] = res
	}

	return mergedProps, nil
}

func valueTypeOf(in interface{}) valueType {
	switch in.(type) {
	case string:
		return textual
	case float64:
		return numerical
	case bool:
		return boolean
	case *models.GeoCoordinates:
		return geo
	default:
		return unknown
	}
}

func mergeTextualProps(in []interface{}) (string, error) {
	var values []string
	seen := make(map[string]struct{}, len(in))
	for i, elem := range in {
		asString, ok := elem.(string)
		if !ok {
			return "", fmt.Errorf("element %d: expected textual element to be string, but got %T", i, elem)
		}

		if _, ok := seen[asString]; ok {
			// this is a duplicate, don't append it again
			continue
		}

		seen[asString] = struct{}{}
		values = append(values, asString)
	}

	if len(values) == 1 {
		return values[0], nil
	}

	return fmt.Sprintf("%s (%s)", values[0], strings.Join(values[1:], ", ")), nil
}

func mergeNumericalProps(in []interface{}) (float64, error) {
	var sum float64
	for i, elem := range in {
		asFloat, ok := elem.(float64)
		if !ok {
			return 0, fmt.Errorf("element %d: expected numerical element to be float64, but got %T", i, elem)
		}

		sum += asFloat
	}

	return sum / float64(len(in)), nil
}

func mergeBooleanProps(in []interface{}) (bool, error) {
	var countTrue uint
	var countFalse uint
	for i, elem := range in {
		asBool, ok := elem.(bool)
		if !ok {
			return false, fmt.Errorf("element %d: expected boolean element to be bool, but got %T", i, elem)
		}

		if asBool {
			countTrue++
		} else {
			countFalse++
		}
	}

	return countTrue >= countFalse, nil
}

func mergeGeoProps(in []interface{}) (*models.GeoCoordinates, error) {
	var sumLat float32
	var sumLon float32

	for i, elem := range in {
		asGeo, ok := elem.(*models.GeoCoordinates)
		if !ok {
			return nil, fmt.Errorf("element %d: expected geo element to be *models.GeoCoordinates, but got %T", i, elem)
		}

		sumLat += asGeo.Latitude
		sumLon += asGeo.Longitude
	}

	return &models.GeoCoordinates{
		Latitude:  sumLat / float32(len(in)),
		Longitude: sumLon / float32(len(in)),
	}, nil
}
