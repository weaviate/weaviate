//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package v1

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func ExtractFilters(filterIn *pb.Filters, authorizedGetClass classGetterWithAuthzFunc, className, tenant string) (filters.Clause, error) {
	returnFilter := filters.Clause{}
	if filterIn.Operator == pb.Filters_OPERATOR_AND || filterIn.Operator == pb.Filters_OPERATOR_OR {
		if filterIn.Operator == pb.Filters_OPERATOR_AND {
			returnFilter.Operator = filters.OperatorAnd
		} else {
			returnFilter.Operator = filters.OperatorOr
		}

		clauses := make([]filters.Clause, len(filterIn.Filters))
		for i, clause := range filterIn.Filters {
			retClause, err := ExtractFilters(clause, authorizedGetClass, className, tenant)
			if err != nil {
				return filters.Clause{}, err
			}
			clauses[i] = retClause
		}

		returnFilter.Operands = clauses

	} else {
		if filterIn.Target == nil && len(filterIn.On)%2 != 1 {
			return filters.Clause{}, fmt.Errorf(
				"paths needs to have a uneven number of components: property, class, property, ...., got %v", filterIn.On,
			)
		}

		switch filterIn.Operator {
		case pb.Filters_OPERATOR_EQUAL:
			returnFilter.Operator = filters.OperatorEqual
		case pb.Filters_OPERATOR_NOT_EQUAL:
			returnFilter.Operator = filters.OperatorNotEqual
		case pb.Filters_OPERATOR_GREATER_THAN:
			returnFilter.Operator = filters.OperatorGreaterThan
		case pb.Filters_OPERATOR_GREATER_THAN_EQUAL:
			returnFilter.Operator = filters.OperatorGreaterThanEqual
		case pb.Filters_OPERATOR_LESS_THAN:
			returnFilter.Operator = filters.OperatorLessThan
		case pb.Filters_OPERATOR_LESS_THAN_EQUAL:
			returnFilter.Operator = filters.OperatorLessThanEqual
		case pb.Filters_OPERATOR_WITHIN_GEO_RANGE:
			returnFilter.Operator = filters.OperatorWithinGeoRange
		case pb.Filters_OPERATOR_LIKE:
			returnFilter.Operator = filters.OperatorLike
		case pb.Filters_OPERATOR_IS_NULL:
			returnFilter.Operator = filters.OperatorIsNull
		case pb.Filters_OPERATOR_CONTAINS_ANY:
			returnFilter.Operator = filters.ContainsAny
		case pb.Filters_OPERATOR_CONTAINS_ALL:
			returnFilter.Operator = filters.ContainsAll
		default:
			return filters.Clause{}, fmt.Errorf("unknown filter operator %v", filterIn.Operator)
		}

		var dataType schema.DataType
		if filterIn.Target == nil {
			path, err := extractPath(className, filterIn.On)
			if err != nil {
				return filters.Clause{}, err
			}
			returnFilter.On = path

			dataType, err = extractDataType(authorizedGetClass, returnFilter.Operator, className, tenant, filterIn.On)
			if err != nil {
				return filters.Clause{}, err
			}
		} else {
			path, dataType2, err := extractPathNew(authorizedGetClass, className, tenant, filterIn.Target, returnFilter.Operator)
			if err != nil {
				return filters.Clause{}, err
			}
			dataType = dataType2
			returnFilter.On = path
		}

		// datatype UUID is just a string
		if dataType == schema.DataTypeUUID {
			dataType = schema.DataTypeText
		}

		var val interface{}
		switch filterIn.TestValue.(type) {
		case *pb.Filters_ValueText:
			val = filterIn.GetValueText()
		case *pb.Filters_ValueInt:
			val = int(filterIn.GetValueInt())
		case *pb.Filters_ValueBoolean:
			val = filterIn.GetValueBoolean()
		case *pb.Filters_ValueNumber:
			val = filterIn.GetValueNumber()
		case *pb.Filters_ValueIntArray:
			// convert from int32 GRPC to go-int
			valInt32 := filterIn.GetValueIntArray().Values
			valInt := make([]int, len(valInt32))
			for i := 0; i < len(valInt32); i++ {
				valInt[i] = int(valInt32[i])
			}
			val = valInt
		case *pb.Filters_ValueTextArray:
			val = filterIn.GetValueTextArray().Values
		case *pb.Filters_ValueNumberArray:
			val = filterIn.GetValueNumberArray().Values
		case *pb.Filters_ValueBooleanArray:
			val = filterIn.GetValueBooleanArray().Values
		case *pb.Filters_ValueGeo:
			valueFilter := filterIn.GetValueGeo()
			val = filters.GeoRange{
				GeoCoordinates: &models.GeoCoordinates{
					Latitude:  &valueFilter.Latitude,
					Longitude: &valueFilter.Longitude,
				},
				Distance: valueFilter.Distance,
			}
		default:
			return filters.Clause{}, fmt.Errorf("unknown value type %v", filterIn.TestValue)
		}

		// correct the type of value when filtering on a float/int property but sending an int/float. This is easy to
		// get wrong
		if number, ok := val.(int); ok && dataType == schema.DataTypeNumber {
			val = float64(number)
		}
		if number, ok := val.(float64); ok && dataType == schema.DataTypeInt {
			val = int(number)
			if float64(int(number)) != number {
				return filters.Clause{}, fmt.Errorf("filtering for integer, but received a floating point number %v", number)
			}
		}

		// correct type for containsXXX in case users send int/float for a float/int array
		if (returnFilter.Operator == filters.ContainsAll || returnFilter.Operator == filters.ContainsAny) && dataType == schema.DataTypeNumber {
			valSlice, ok := val.([]int)
			if ok {
				val64 := make([]float64, len(valSlice))
				for i := 0; i < len(valSlice); i++ {
					val64[i] = float64(valSlice[i])
				}
				val = val64
			}
		}

		if (returnFilter.Operator == filters.ContainsAll || returnFilter.Operator == filters.ContainsAny) && dataType == schema.DataTypeInt {
			valSlice, ok := val.([]float64)
			if ok {
				valInt := make([]int, len(valSlice))
				for i := 0; i < len(valSlice); i++ {
					if float64(int(valSlice[i])) != valSlice[i] {
						return filters.Clause{}, fmt.Errorf("filtering for integer, but received a floating point number %v", valSlice[i])
					}
					valInt[i] = int(valSlice[i])
				}
				val = valInt
			}
		}

		value := filters.Value{Value: val, Type: dataType}
		returnFilter.Value = &value

	}
	return returnFilter, nil
}

func extractDataTypeProperty(authorizedGetClass classGetterWithAuthzFunc, operator filters.Operator, className, tenant string, on []string) (schema.DataType, error) {
	var dataType schema.DataType
	if operator == filters.OperatorIsNull {
		dataType = schema.DataTypeBoolean
	} else if len(on) > 1 {
		propToCheck := on[len(on)-1]
		_, isPropLengthFilter := schema.IsPropertyLength(propToCheck, 0)
		if isPropLengthFilter {
			return schema.DataTypeInt, nil
		}

		classOfProp := on[len(on)-2]
		class, err := authorizedGetClass(classOfProp)
		if err != nil {
			return dataType, err
		}
		prop, err := schema.GetPropertyByName(class, propToCheck)
		if err != nil {
			return dataType, err
		}
		dataType = schema.DataType(prop.DataType[0])
	} else {
		propToCheck := on[0]
		_, isPropLengthFilter := schema.IsPropertyLength(propToCheck, 0)
		if isPropLengthFilter {
			return schema.DataTypeInt, nil
		}

		class, err := authorizedGetClass(className)
		if err != nil {
			return dataType, err
		}
		if class == nil {
			return dataType, fmt.Errorf("could not find class %s in schema", className)
		}
		prop, err := schema.GetPropertyByName(class, propToCheck)
		if err != nil {
			return dataType, err
		}
		if schema.IsRefDataType(prop.DataType) {
			// This is a filter on a reference property without a path so is counting
			// the number of references. Needs schema.DataTypeInt: entities/filters/filters_validator.go#L116-L127
			return schema.DataTypeInt, nil
		}
		dataType = schema.DataType(prop.DataType[0])
	}

	// searches on array datatypes always need the base-type as value-type
	if baseType, isArray := schema.IsArrayType(dataType); isArray {
		return baseType, nil
	}
	return dataType, nil
}

func extractDataType(authorizedGetClass classGetterWithAuthzFunc, operator filters.Operator, classname, tenant string, on []string) (schema.DataType, error) {
	propToFilterOn := on[len(on)-1]
	if propToFilterOn == filters.InternalPropID {
		return schema.DataTypeText, nil
	} else if propToFilterOn == filters.InternalPropCreationTimeUnix || propToFilterOn == filters.InternalPropLastUpdateTimeUnix {
		return schema.DataTypeDate, nil
	} else {
		return extractDataTypeProperty(authorizedGetClass, operator, classname, tenant, on)
	}
}

func extractPath(className string, on []string) (*filters.Path, error) {
	if len(on) > 1 {
		var err error
		child, err := extractPath(on[1], on[2:])
		if err != nil {
			return nil, err
		}
		return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(on[0]), Child: child}, nil

	}
	return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(on[0]), Child: nil}, nil
}

func extractPathNew(authorizedGetClass classGetterWithAuthzFunc, className, tenant string, target *pb.FilterTarget, operator filters.Operator) (*filters.Path, schema.DataType, error) {
	class, err := authorizedGetClass(className)
	if err != nil {
		return nil, "", err
	}
	switch target.Target.(type) {
	case *pb.FilterTarget_Property:
		dt, err := extractDataType(authorizedGetClass, operator, className, tenant, []string{target.GetProperty()})
		if err != nil {
			return nil, "", err
		}
		return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(target.GetProperty()), Child: nil}, dt, nil
	case *pb.FilterTarget_SingleTarget:
		singleTarget := target.GetSingleTarget()
		normalizedRefPropName := schema.LowercaseFirstLetter(singleTarget.On)
		refProp, err := schema.GetPropertyByName(class, normalizedRefPropName)
		if err != nil {
			return nil, "", err
		}
		if len(refProp.DataType) != 1 {
			return nil, "", fmt.Errorf("expected reference property with a single target, got %v for %v ", refProp.DataType, refProp.Name)
		}
		child, property, err := extractPathNew(authorizedGetClass, refProp.DataType[0], tenant, singleTarget.Target, operator)
		if err != nil {
			return nil, "", err
		}
		return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(normalizedRefPropName), Child: child}, property, nil
	case *pb.FilterTarget_MultiTarget:
		multiTarget := target.GetMultiTarget()
		child, property, err := extractPathNew(authorizedGetClass, multiTarget.TargetCollection, tenant, multiTarget.Target, operator)
		if err != nil {
			return nil, "", err
		}
		return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(schema.LowercaseFirstLetter(multiTarget.On)), Child: child}, property, nil
	case *pb.FilterTarget_Count:
		count := target.GetCount()
		return &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(schema.LowercaseFirstLetter(count.On)), Child: nil}, schema.DataTypeInt, nil
	default:
		return nil, "", fmt.Errorf("unknown target type %v", target)
	}
}
