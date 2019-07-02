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
)
