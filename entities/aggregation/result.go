package aggregation

type Result struct {
	Groups []Group
}

type Group struct {
	Properties map[string]Property
	GroupedBy  GroupedBy
}

type Property struct {
	Type                  PropertyType
	NumericalAggregations map[string]float64
	TextAggregations      map[string][]TextOccurence
}

type PropertyType string

const (
	Numerical PropertyType = "numerical"
	Text      PropertyType = "text"
)

type GroupedBy struct {
	Value interface{}
	Path  []string
}

type TextOccurence struct {
	Value  string
	Occurs int
}
