package network_get

import (
	"reflect"
	"testing"
)

var availableInstances = []string{"weaviateB", "weaviateC"}

func TestNoFilters(t *testing.T) {
	args := map[string]interface{}{}

	result, err := FiltersForNetworkInstances(args, availableInstances)

	if !reflect.DeepEqual(result, FiltersPerInstance{}) {
		t.Errorf("expected an empty filters per instance, but got %#v", result)
	}

	if err != nil {
		t.Errorf("expected FiltersForNetworkInstances not to error, but got %s", err)
	}
}

func TestSingleInstanceWithSingleFilterAsOperands(t *testing.T) {
	args := map[string]interface{}{
		"where": map[string]interface{}{
			"operator": "And",
			"operands": []interface{}{
				map[string]interface{}{
					"path":     []interface{}{"weaviateB", "Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				},
			},
		},
	}
	expectedResult := FiltersPerInstance{
		"weaviateB": map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "And",
				"operands": []map[string]interface{}{{
					"path":     []string{"Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				}},
			},
		},
	}

	result, err := FiltersForNetworkInstances(args, availableInstances)

	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("expected result to be \n\n%#v\n\n, but got \n\n%#v\n\n", expectedResult, result)
	}

	if err != nil {
		t.Errorf("expected FiltersForNetworkInstances not to error, but got %s", err)
	}
}

func TestSingleInstanceWithSingleAsOperator(t *testing.T) {
	args := map[string]interface{}{
		"where": map[string]interface{}{
			"operator":    "NotEqual",
			"valueString": "Berlin",
			"path":        []interface{}{"weaviateB", "Things", "City", "name"},
		},
	}
	expectedResult := FiltersPerInstance{
		"weaviateB": map[string]interface{}{
			"where": map[string]interface{}{
				"operator":    "NotEqual",
				"valueString": "Berlin",
				"path":        []string{"Things", "City", "name"},
			},
		},
	}

	result, err := FiltersForNetworkInstances(args, availableInstances)

	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("expected result to be \n\n%#v\n\n, but got \n\n%#v\n\n", expectedResult, result)
	}

	if err != nil {
		t.Errorf("expected FiltersForNetworkInstances not to error, but got %s", err)
	}
}

func TestSingleInstanceWithSingleWrongInstanceNameInPath(t *testing.T) {
	args := map[string]interface{}{
		"where": map[string]interface{}{
			"operator":    "NotEqual",
			"valueString": "Berlin",
			"path":        []interface{}{"weaviateX", "Things", "City", "name"},
		},
	}

	_, err := FiltersForNetworkInstances(args, availableInstances)

	if err == nil {
		t.Error("expected FiltersForNetworkInstances to error, but got nothing")
	}
}

func TestTwoInstancesWithSingleFilterEach(t *testing.T) {
	args := map[string]interface{}{
		"where": map[string]interface{}{
			"operator": "And",
			"operands": []interface{}{
				map[string]interface{}{
					"path":     []interface{}{"weaviateB", "Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				},
				map[string]interface{}{
					"path":     []interface{}{"weaviateC", "Things", "Airports", "capacity"},
					"operator": "LessThan",
					"valueInt": 60000,
				},
			},
		},
	}
	expectedResult := FiltersPerInstance{
		"weaviateB": map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "And",
				"operands": []map[string]interface{}{{
					"path":     []string{"Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				}},
			},
		},
		"weaviateC": map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "And",
				"operands": []map[string]interface{}{{
					"path":     []string{"Things", "Airports", "capacity"},
					"operator": "LessThan",
					"valueInt": 60000,
				}},
			},
		},
	}

	result, err := FiltersForNetworkInstances(args, availableInstances)

	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("expected result to be \n\n%#v\n\n, but got \n\n%#v\n\n", expectedResult, result)
	}

	if err != nil {
		t.Errorf("expected FiltersForNetworkInstances not to error, but got %s", err)
	}
}

func TestOneInstanceWithTwoFilters(t *testing.T) {
	args := map[string]interface{}{
		"where": map[string]interface{}{
			"operator": "And",
			"operands": []interface{}{
				map[string]interface{}{
					"path":     []interface{}{"weaviateB", "Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				},
				map[string]interface{}{
					"path":        []interface{}{"weaviateB", "Things", "City", "name"},
					"operator":    "NotEqual",
					"valueString": "Berlin",
				},
			},
		},
	}
	expectedResult := FiltersPerInstance{
		"weaviateB": map[string]interface{}{
			"where": map[string]interface{}{
				"operator": "And",
				"operands": []map[string]interface{}{{
					"path":     []string{"Things", "City", "population"},
					"operator": "GreaterThan",
					"valueInt": 1000000,
				}, {
					"path":        []string{"Things", "City", "name"},
					"operator":    "NotEqual",
					"valueString": "Berlin",
				}},
			},
		},
	}

	result, err := FiltersForNetworkInstances(args, availableInstances)

	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("expected result to be \n\n%#v\n\n, but got \n\n%#v\n\n", expectedResult, result)
	}

	if err != nil {
		t.Errorf("expected FiltersForNetworkInstances not to error, but got %s", err)
	}
}
