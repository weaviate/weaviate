package schema

const (
	ErrRefToNonexistentClass string = "reference property to nonexistent class"
	ErrorNoSuchClass         string = "no such class with name '%s' found in the schema. Check your schema files for which classes are available"
	ErrorNoSuchProperty      string = "no such prop with name '%s' found in class '%s' in the schema. Check your schema files for which properties in this class are available"
	ErrorNoSuchDatatype      string = "given value-DataType does not exist."
)
