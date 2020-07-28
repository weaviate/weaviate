package multi

import "github.com/semi-technologies/weaviate/entities/schema/kind"

type Identifier struct {
	ID               string
	Kind             kind.Kind
	ClassName        string
	OriginalPosition int
}
