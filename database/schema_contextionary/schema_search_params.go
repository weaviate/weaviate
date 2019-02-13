package schema

import (
	"fmt"

	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/fatih/camelcase"
)

// SearchType to search for either class names or property names
type SearchType string

const (
	// SearchTypeClass to search the contextionary for class names
	SearchTypeClass SearchType = "class"
	// SearchTypeProperty to search the contextionary for property names
	SearchTypeProperty SearchType = "property"
)

// SearchParams to be used for a SchemaSearch. See individual properties for
// additional documentation on what they do
type SearchParams struct {
	// SearchType can be SearchTypeClass or SearchTypeProperty
	SearchType SearchType

	// Name is the string-representation of the class or property name
	Name string

	// Keywords (optional). If no keywords are specified, only the class name
	// will be used as a search query.
	Keywords models.SemanticSchemaKeywords

	// Kind as in Thing or Class, not required if SearchType == SearchTypeProperty
	Kind kind.Kind

	// Certaintiy must be a value between 0 and 1. The higher it is the narrower
	// is the search, the lower it is, the wider the search is
	Certainty float32
}

// Validate the feasibility of the specified arguments
func (p SearchParams) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("Name cannot be empty")
	}

	if err := p.validateCertaintyOrWeight(p.Certainty); err != nil {
		return fmt.Errorf("invalid Certainty: %s", err)
	}

	if p.SearchType != SearchTypeClass && p.SearchType != SearchTypeProperty {
		return fmt.Errorf(
			"SearchType must be SearchTypeClass or SearchTypeProperty, but got '%s'", p.SearchType)
	}

	if p.Kind == "" {
		return fmt.Errorf("Kind cannot be empty")
	}

	for i, keyword := range p.Keywords {
		if err := p.validateKeyword(keyword); err != nil {
			return fmt.Errorf("invalid keyword at position %d: %s", i, err)
		}
	}

	return nil
}

func (p SearchParams) validateKeyword(kw *models.SemanticSchemaKeywordsItems0) error {
	if kw.Keyword == "" {
		return fmt.Errorf("Keyword cannot be empty")
	}

	if len(camelcase.Split(kw.Keyword)) > 1 {
		return fmt.Errorf("invalid Keyword: keywords cannot be camelCased - "+
			"instead split your keyword up into several keywords, this way each word "+
			"of your camelCased string can have its own weight, got '%s'", kw.Keyword)
	}

	if err := p.validateCertaintyOrWeight(kw.Weight); err != nil {
		return fmt.Errorf("invalid Weight: %s", err)
	}

	return nil
}

func (p SearchParams) validateCertaintyOrWeight(c float32) error {
	if c >= 0 && c <= 1 {
		return nil
	}

	return fmt.Errorf("must be between 0 and 1, but got '%f'", c)
}
