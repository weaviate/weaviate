package schema

import "github.com/creativesoftwarefdn/weaviate/contextionary"

// Contextionary composes a regular contextionary with additional
// schema-related query methods
type Contextionary struct {
	contextionary.Contextionary
}

// New creates a new Contextionary from a contextionary.Contextionary which it
// extends with Schema-related search methods
func New(c contextionary.Contextionary) *Contextionary {
	return &Contextionary{
		Contextionary: c,
	}
}
