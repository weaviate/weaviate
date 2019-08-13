package aggregation

type Result struct {
	Groups []Group
}

type Group struct {
	Properties map[string][]Property
	GroupedBy  GroupedBy
}

type GroupedBy struct {
	Value interface{}
	Path  []string
}

type Property interface {
	IsNumerical() bool
	AsNumerical() Numerical
	IsText() bool
	AsText() Text
}

type Numerical struct {
	Aggregator string
	Value      float64
}

func (n Numerical) IsNumerical() bool {
	return true
}

func (n Numerical) IsText() bool {
	return false
}

func (n Numerical) AsNumerical() Numerical {
	return n
}

func (n Numerical) AsText() Text {
	panic("is numerical aggregation")
}

type Text struct {
	Occurences []TextOccurence
}

func (s Text) IsText() bool {
	return true
}

func (s Text) IsNumerical() bool {
	return false
}

func (s Text) AsText() Text {
	return s
}

func (s Text) AsNumerical() Numerical {
	panic("is text aggregation")
}

type TextOccurence struct {
	Value  string
	Occurs int
}
