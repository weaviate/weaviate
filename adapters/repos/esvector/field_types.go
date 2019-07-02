package esvector

// FieldType in Elasticsearch
// These should be provided by the official es go client, but couldn't be found
// in there
type FieldType string

const (

	// Text indexes a block of text, such as the body of an email
	Text FieldType = "text"

	// Keyword indexes an exact string, such as an email address
	Keyword FieldType = "keyword"

	// Integer indexes an integer
	Integer FieldType = "integer"

	// Float indexes a float64
	Float FieldType = "float"

	// Boolean indexes a boolean
	Boolean FieldType = "boolean"

	// Date indexes a date
	Date FieldType = "date"

	// GeoPoint indexes a geo point
	GeoPoint FieldType = "geo_point"
)
